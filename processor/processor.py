"""Processor: reads aircraft positions from Kafka, checks and cleans them,
stores them in Cassandra, and detects circling, emergencies and Hamburg movements.

One cycle:
  1. Read a batch of up to BATCH_SIZE messages from Kafka.
  2. Normalize and validate them (rules.py). Invalid ones go to the dead-letter topic.
  3. Write the valid positions to Cassandra.
  4. Detect circling, emergencies and movements; write alerts and the live view.
  5. Write one row of batch statistics (throughput, latency, data quality).
  6. Commit the Kafka offsets. Only now does the batch count as done.

If storing keeps failing, the processor exits WITHOUT committing. Docker restarts
it and it reads the same batch again. Writing the same rows again changes nothing,
because every table is keyed so that a repeat overwrites itself (upsert).
"""

import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timedelta, timezone

from cassandra import ConsistencyLevel
from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from confluent_kafka import Consumer, Producer

import rules

# --- Settings --------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka-1:19092,kafka-2:19092,kafka-3:19092")
TOPIC = os.getenv("KAFKA_TOPIC", "adsb.positions")
DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "adsb.dlq")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "processor")
CASSANDRA_HOSTS = os.getenv("CASSANDRA_HOSTS", "cassandra-1,cassandra-2,cassandra-3").split(",")
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "airspace")
LOCAL_DC = os.getenv("CASSANDRA_DC", "dc1")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))          # max messages per batch
BATCH_TIMEOUT_S = float(os.getenv("BATCH_TIMEOUT_S", "1"))  # max wait to fill a batch
POLL_INTERVAL_S = float(os.getenv("POLL_INTERVAL_S", "5"))  # the producer's interval
CIRCLE_WINDOW = timedelta(minutes=int(os.getenv("CIRCLE_WINDOW_MIN", "10")))
CIRCLE_DEGREES = float(os.getenv("CIRCLE_DEGREES", "360"))
MOVEMENT_RADIUS_KM = float(os.getenv("MOVEMENT_RADIUS_KM", "5"))
AIRPORT = "HAM"
WRITE_ATTEMPTS = 3          # tries per batch before exiting for a clean restart

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.getLogger("cassandra").setLevel(logging.WARNING)   # keep the driver quiet
log = logging.getLogger("processor")

running = True


def request_stop(signum, frame):
    global running
    running = False


signal.signal(signal.SIGTERM, request_stop)
signal.signal(signal.SIGINT, request_stop)


def utc(moment):
    """The Cassandra driver returns times without a time zone; they are UTC."""
    return moment.replace(tzinfo=timezone.utc) if moment.tzinfo is None else moment


def parse_time(text):
    try:
        return datetime.fromisoformat(text).astimezone(timezone.utc)
    except (TypeError, ValueError):
        return None


# --- Cassandra ---------------------------------------------------------------------
def connect_cassandra():
    """Connect to the cluster, retrying while the nodes are still starting."""
    profile = ExecutionProfile(
        load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy(local_dc=LOCAL_DC)),
        consistency_level=ConsistencyLevel.LOCAL_QUORUM,   # 2 of 3 replicas must answer
        request_timeout=10,
    )
    for attempt in range(1, 31):
        try:
            cluster = Cluster(CASSANDRA_HOSTS, execution_profiles={EXEC_PROFILE_DEFAULT: profile})
            return cluster, cluster.connect(KEYSPACE)
        except Exception as exc:                            # nodes still starting up
            log.warning("Cassandra not ready (attempt %d/30): %s", attempt, exc)
            time.sleep(5)
    sys.exit("Cassandra not reachable, giving up")


def prepare(session):
    """Prepared statements: parsed once by Cassandra, then reused with new values."""
    q = session.prepare
    return {
        "position": q(
            "INSERT INTO positions_by_aircraft (hex, day, pos_time, flight, registration, aircraft_type, "
            "category, lat, lon, alt_baro_ft, on_ground, gs_kt, track_deg, true_heading_deg, squawk, "
            "emergency, seen_pos_s, is_stale, source, schema_version, ingested_at, processed_at, "
            "kafka_partition, kafka_offset) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        ),
        "track": q(
            "SELECT pos_time, track_deg, on_ground, is_stale, squawk, emergency FROM positions_by_aircraft "
            "WHERE hex = ? AND day = ? AND pos_time >= ? AND pos_time <= ?"
        ),
        "recent_alerts": q("SELECT hex, alert_type FROM alerts_by_day WHERE day = ? AND alert_time >= ?"),
        "alert": q(
            "INSERT INTO alerts_by_day (day, alert_time, hex, alert_type, flight, lat, lon, alt_baro_ft, detail) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
        ),
        "movement": q(
            "INSERT INTO movements_by_day (airport, day, window_start, hex, movement, event_time, flight) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)"
        ),
        "live": q(
            "INSERT INTO live_by_minute (minute, hex, flight, lat, lon, alt_baro_ft, on_ground, gs_kt, "
            "track_deg, turn_deg, squawk, emergency, pos_time, category, is_airliner) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        ),
        "stats": q(
            "INSERT INTO batch_stats_by_day (day, batch_time, kafka_partition, first_offset, records_in, "
            "records_valid, records_dlq, records_excluded, records_stale, duplicates, missing_track, "
            "missing_speed, missing_altitude, implausible, latency_p50_ms, latency_p95_ms, latency_max_ms) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        ),
    }


def write_all(session, statement, rows):
    """Run many writes in parallel; raise if any of them failed."""
    if not rows:
        return []
    results = execute_concurrent_with_args(session, statement, rows, concurrency=50, raise_on_first_error=True)
    return [result for success, result in results]


# --- One batch ---------------------------------------------------------------------
def position_row(record, message, processed_at):
    altitude = record["alt_baro_ft"]
    return (
        record["hex"], record["pos_time"].date(), record["pos_time"], record["flight"],
        record["registration"], record["aircraft_type"], record["category"], record["lat"], record["lon"],
        int(round(altitude)) if altitude is not None else None, record["on_ground"], record["gs_kt"],
        record["track_deg"], record["true_heading_deg"], record["squawk"], record["emergency"],
        record["seen_pos_s"], record["is_stale"], record["source"], record["schema_version"],
        parse_time(record["ingested_at"]), processed_at, message.partition(), message.offset(),
    )


def detect(session, statements, records):
    """Run the detection rules for the newest position of each aircraft in the batch.

    Returns rows for the alerts, movements and live tables.
    """
    newest = {}
    for record in records:
        if record["hex"] not in newest or record["pos_time"] > newest[record["hex"]]["pos_time"]:
            newest[record["hex"]] = record
    if not newest:
        return [], [], []

    # Alerts raised in the last window, so each episode is reported once.
    earliest = min(r["pos_time"] for r in newest.values()) - CIRCLE_WINDOW
    latest = max(r["pos_time"] for r in newest.values())
    already_alerted = set()
    for day in rules.days_between(earliest, latest):
        for row in session.execute(statements["recent_alerts"], (day, earliest)):
            already_alerted.add((row.hex, row.alert_type))

    # Fetch the last 10 minutes of track for every aircraft that needs it, in parallel.
    def needs_history(r):
        near_airport = rules.distance_km(r["lat"], r["lon"], rules.HAM_LAT, rules.HAM_LON) <= MOVEMENT_RADIUS_KM
        signals_emergency = rules.emergency_reason(r["squawk"], r["emergency"]) is not None
        return near_airport or signals_emergency or (not r["on_ground"] and not r["is_stale"])

    queries, owners = [], []
    for r in newest.values():
        if needs_history(r):
            start = r["pos_time"] - CIRCLE_WINDOW
            for day in rules.days_between(start, r["pos_time"]):
                queries.append((r["hex"], day, start, r["pos_time"]))
                owners.append(r["hex"])
    history = {}
    for hex_id, rows in zip(owners, write_all(session, statements["track"], queries)):
        history.setdefault(hex_id, []).extend(rows)

    alerts, movements, live = [], [], []
    for hex_id, r in newest.items():
        track = sorted(history.get(hex_id, []), key=lambda row: row.pos_time)
        airborne_samples = [
            (utc(row.pos_time), row.track_deg) for row in track if not row.on_ground and not row.is_stale
        ]
        turn = rules.total_turn(airborne_samples)
        day = r["pos_time"].date()
        altitude = int(round(r["alt_baro_ft"])) if r["alt_baro_ft"] is not None else None

        airliner = rules.is_airliner(r["category"], r["aircraft_type"])
        if (abs(turn) >= CIRCLE_DEGREES and airliner
                and (hex_id, "circling") not in already_alerted):
            direction = "right" if turn > 0 else "left"
            alerts.append((day, r["pos_time"], hex_id, "circling", r["flight"], r["lat"], r["lon"], altitude,
                           f"turned {abs(turn):.0f} degrees to the {direction} within {CIRCLE_WINDOW.seconds // 60} minutes"))

        recent = [(utc(row.pos_time), row.squawk, row.emergency) for row in track]
        reason = rules.confirmed_emergency(recent, r["pos_time"])      # needs 3 positions in 2 minutes
        if reason and (hex_id, "emergency") not in already_alerted:
            alerts.append((day, r["pos_time"], hex_id, "emergency", r["flight"], r["lat"], r["lon"], altitude, reason))

        earlier = [row for row in track if utc(row.pos_time) < r["pos_time"]]
        if earlier and rules.distance_km(r["lat"], r["lon"], rules.HAM_LAT, rules.HAM_LON) <= MOVEMENT_RADIUS_KM:
            kind = rules.movement(earlier[-1].on_ground, r["on_ground"])
            if kind:
                movements.append((AIRPORT, day, rules.window_start(r["pos_time"]), hex_id, kind,
                                  r["pos_time"], r["flight"]))

        minute = r["pos_time"].replace(second=0, microsecond=0)
        live.append((minute, hex_id, r["flight"], r["lat"], r["lon"], altitude, r["on_ground"], r["gs_kt"],
                     r["track_deg"], turn, r["squawk"], r["emergency"], r["pos_time"], r["category"], airliner))
    return alerts, movements, live


def process_batch(messages, session, statements, dlq):
    processed_at = datetime.now(timezone.utc)

    # 1. Parse and normalize. Unreadable JSON goes straight to the dead-letter topic.
    parsed, rejected = [], []
    for message in messages:
        try:
            parsed.append((rules.normalize(json.loads(message.value())), message))
        except (TypeError, ValueError):
            rejected.append((message, ["message:invalid JSON"]))

    # 2. Validate with the Pandera schema.
    valid_positions, reasons = rules.validate([record for record, _ in parsed])
    for position, why in reasons.items():
        rejected.append((parsed[position][1], why))
    implausible = sum(1 for why in reasons.values() if rules.is_implausible(why))
    valid = [parsed[i] for i in valid_positions]
    usable = [(r, m) for r, m in valid if not r["is_ground_vehicle"]]
    excluded = len(valid) - len(usable)

    # 3. Dead-letter topic: keep the original message and the reasons.
    for message, why in rejected:
        dlq.produce(DLQ_TOPIC, key=message.key(), value=json.dumps({
            "reasons": why,
            "source_partition": message.partition(),
            "source_offset": message.offset(),
            "raw": (message.value() or b"").decode("utf-8", "replace"),
        }))

    # 4. Store positions, then run detection on what is stored.
    records = [r for r, _ in usable]
    write_all(session, statements["position"], [position_row(r, m, processed_at) for r, m in usable])
    alerts, movements, live = detect(session, statements, records)
    write_all(session, statements["alert"], alerts)
    write_all(session, statements["movement"], movements)
    write_all(session, statements["live"], live)

    # 5. Batch statistics. The row key comes from the first message, so a re-run
    #    of the same batch overwrites its own row.
    first = messages[0]
    batch_time = datetime.fromtimestamp(first.timestamp()[1] / 1000, tz=timezone.utc)
    airborne = [r for r in records if not r["on_ground"]]
    latencies_ms = [(processed_at - r["pos_time"]).total_seconds() * 1000 for r in records]
    write_all(session, statements["stats"], [(
        batch_time.date(), batch_time, first.partition(), first.offset(),
        len(messages), len(valid), len(rejected), excluded,
        sum(r["is_stale"] for r in records),
        sum(1 for r in records if r["seen_pos_s"] is not None and r["seen_pos_s"] >= POLL_INTERVAL_S),
        sum(r["track_deg"] is None for r in airborne),
        sum(r["gs_kt"] is None for r in records),
        sum(r["alt_baro_ft"] is None for r in airborne),
        implausible,
        rules.percentile(latencies_ms, 0.5), rules.percentile(latencies_ms, 0.95),
        max(latencies_ms) if latencies_ms else None,
    )])

    # 6. Make sure the dead-letter messages are safely stored before we commit.
    if dlq.flush(10) > 0:
        raise RuntimeError("dead-letter messages not confirmed by Kafka")

    return {"in": len(messages), "stored": len(usable), "dlq": len(rejected), "excluded": excluded,
            "alerts": len(alerts), "movements": len(movements)}


# --- Main loop ---------------------------------------------------------------------
def main():
    cluster, session = connect_cassandra()
    statements = prepare(session)

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,               # all processor instances share the work
        "enable.auto.commit": False,        # we commit ourselves, after storing
        "auto.offset.reset": "earliest",    # a brand-new group starts at the oldest message
    })
    dlq = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "client.id": "processor-dlq",
        "acks": "all",
        "enable.idempotence": True,
    })
    consumer.subscribe([TOPIC])
    log.info("Processor started: %s -> Cassandra keyspace %s (group %s)", TOPIC, KEYSPACE, GROUP_ID)

    while running:
        batch = consumer.consume(num_messages=BATCH_SIZE, timeout=BATCH_TIMEOUT_S)
        messages = []
        for message in batch:
            if message.error():
                log.warning("Kafka error: %s", message.error())
            else:
                messages.append(message)
        if not messages:
            continue

        for attempt in range(1, WRITE_ATTEMPTS + 1):
            try:
                summary = process_batch(messages, session, statements, dlq)
                break
            except Exception as exc:
                log.warning("Batch failed (attempt %d/%d): %s: %s", attempt, WRITE_ATTEMPTS, type(exc).__name__, exc)
                time.sleep(2 ** attempt)
        else:
            # Never skip data: exit without committing; Docker restarts us and we re-read this batch.
            log.error("Giving up on this batch; exiting without commit so it is re-read after restart")
            consumer.close()
            cluster.shutdown()
            sys.exit(1)

        consumer.commit(asynchronous=False)         # the batch is done only now
        log.info("Batch: %(in)d in, %(stored)d stored, %(dlq)d to DLQ, %(excluded)d ground vehicles, "
                 "%(alerts)d alerts, %(movements)d movements", summary)

    consumer.close()
    cluster.shutdown()
    log.info("Processor stopped")


if __name__ == "__main__":
    main()

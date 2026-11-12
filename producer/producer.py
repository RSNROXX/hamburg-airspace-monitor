"""Producer: polls the adsb.lol API and publishes one Kafka message per
aircraft.

Flow per cycle (every POLL_INTERVAL_S seconds):
  1. Ask adsb.lol for all aircraft within RADIUS_NM of the centre point.
  2. Turn each aircraft into one JSON message, keyed by its ID (hex).
  3. Send all messages to Kafka and wait until Kafka confirms them.
  4. Sleep for the rest of the interval.
If the source fails, wait 1, 2, 4 ... up to 60 seconds before trying again.
"""

import json
import logging
import os
import signal
import time
from datetime import UTC, datetime

import requests
from confluent_kafka import Producer

# --- Settings: read from environment variables, with safe defaults ----------
CENTER_LAT = float(os.getenv("CENTER_LAT", "53.63"))   # Hamburg Airport
CENTER_LON = float(os.getenv("CENTER_LON", "9.99"))
RADIUS_NM = int(os.getenv("RADIUS_NM", "250"))         # nautical miles
POLL_INTERVAL_S = float(os.getenv("POLL_INTERVAL_S", "5"))
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP",
                            "kafka-1:19092,kafka-2:19092,kafka-3:19092")
TOPIC = os.getenv("KAFKA_TOPIC", "adsb.positions")

SOURCE_URL = f"https://api.adsb.lol/v2/lat/{CENTER_LAT}/lon/{CENTER_LON}/dist/{RADIUS_NM}"
SCHEMA_VERSION = 1          # raise this if the message format ever changes
MAX_BACKOFF_S = 60          # longest wait between retries when the source is down
REQUEST_TIMEOUT_S = 10      # give up on one HTTP request after this long

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("producer")

# --- Graceful shutdown: `docker stop` sends SIGTERM; finish cleanly ---------
running = True


def request_stop(signum, frame):
    global running
    running = False


signal.signal(signal.SIGTERM, request_stop)
signal.signal(signal.SIGINT, request_stop)


def wait(seconds):
    """Sleep in small steps so a stop request is noticed quickly."""
    end = time.monotonic() + seconds
    while running and time.monotonic() < end:
        time.sleep(min(0.5, end - time.monotonic()))


def to_iso(epoch_seconds):
    """Convert seconds since 1970 to an ISO-8601 UTC string,
       e.g. 2026-11-11T15:30:00+00:00."""
    return datetime.fromtimestamp(
        epoch_seconds,
        tz=UTC).isoformat()


def build_messages(response):
    """Turn one API response into (key, message) pairs, one per aircraft.

    The aircraft fields are passed on unchanged (raw data); the processor
    validates and cleans them. We only add metadata about where and when
    the record came from.
    """
    server_now_s = response["now"] / 1000          # API gives milliseconds
    ingested_at = to_iso(time.time())
    poll_id = response["now"]                      # same for every aircraft in this poll

    for aircraft in response["ac"]:
        seen_pos = aircraft.get("seen_pos")         # seconds since the position was received
        if isinstance(seen_pos, (int, float)):
            event_time = to_iso(server_now_s - seen_pos)
        else:
            event_time = None                       # no position: the processor will reject it

        message = {
            **aircraft,                             # all original fields
            "source": "adsb.lol",
            "schema_version": SCHEMA_VERSION,
            "poll_id": poll_id,
            "ingested_at": ingested_at,
            "event_time": event_time,
        }
        yield aircraft.get("hex"), message


# --- Delivery reports: Kafka tells us, per message, whether it was stored ---
stats = {"delivered": 0, "failed": 0}


def on_delivery(err, msg):
    if err is not None:     # handle the error 1st
        stats["failed"] += 1
        log.error("Delivery failed for aircraft %s: %s",
                  msg.key(),
                  err)
    else:
        stats["delivered"] += 1


def fetch(session):
    """Fetch one snapshot from the API. Raises an exception if anything is
    wrong."""
    response = session.get(SOURCE_URL, timeout=REQUEST_TIMEOUT_S)
    response.raise_for_status()                     # HTTP 4xx/5xx -> exception
    data = response.json()                          # invalid JSON -> exception
    if "now" not in data or "ac" not in data:
        raise ValueError("unexpected response format")
    return data


def main():
    """ Main funtion/loop of this file."""
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "client.id": "adsb-producer",
        "acks": "all",                # wait for all in-sync replicas
        "enable.idempotence": True,   # retries never create duplicates in Kafka
        "linger.ms": 50,              # wait up to 50 ms to send messages in batches
    })
    session = requests.Session()
    session.headers["User-Agent"] = "hamburg-airspace-monitor/1.0 \
        (university project)"

    log.info("Polling %s every %.0f s into topic %s",
             SOURCE_URL,
             POLL_INTERVAL_S,
             TOPIC)
    backoff = 1

    while running:
        cycle_start = time.monotonic()

        try:
            data = fetch(session)
        except (requests.RequestException, ValueError) as exc:
            log.warning("Source unavailable (%s); retrying in %d s",
                        exc,
                        backoff)
            wait(backoff)
            backoff = min(backoff * 2,
                          MAX_BACKOFF_S)
            continue
        backoff = 1                           # source is back: reset the wait

        sent = 0
        for key, message in build_messages(data):
            value = json.dumps(message)
            try:
                producer.produce(
                    TOPIC,
                    key=key,
                    value=value,
                    on_delivery=on_delivery)
            except BufferError:
                # Local send queue is full (Kafka unreachable for a while): let it drain, retry once.
                producer.poll(1)
                producer.produce(
                    TOPIC,
                    key=key,
                    value=value,
                    on_delivery=on_delivery)
            producer.poll(0)                        # handle delivery reports that are ready
            sent += 1

        not_confirmed = producer.flush(REQUEST_TIMEOUT_S)   # wait for Kafka's confirmations
        log.info(
            "Poll %s: %d aircraft sent, %d not yet confirmed \
                (total delivered %d, failed %d)",
            data["now"], sent, not_confirmed, stats["delivered"],
            stats["failed"],
        )

        wait(max(0.0, POLL_INTERVAL_S - (time.monotonic() - cycle_start)))

    producer.flush(REQUEST_TIMEOUT_S)
    log.info("Producer stopped (delivered %d, failed %d)",
             stats["delivered"],
             stats["failed"])


if __name__ == "__main__":
    main()

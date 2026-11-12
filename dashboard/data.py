"""Read-only Cassandra queries for the dashboard. Every function returns a pandas DataFrame.

All times in Cassandra are UTC. The dashboard converts them to local time only for display.
"""

import os
from datetime import timedelta

import pandas as pd
from cassandra import ConsistencyLevel
from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from cassandra.query import dict_factory

CASSANDRA_HOSTS = os.getenv("CASSANDRA_HOSTS", "cassandra-1,cassandra-2,cassandra-3").split(",")
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "airspace")
LOCAL_DC = os.getenv("CASSANDRA_DC", "dc1")
AIRPORT = "HAM"

STATS_COLUMNS = ["batch_time", "records_in", "records_valid", "records_dlq", "records_excluded",
                 "records_stale", "duplicates", "missing_track", "missing_speed", "missing_altitude",
                 "implausible", "latency_p50_ms", "latency_p95_ms"]
LIVE_COLUMNS = ["hex", "flight", "category", "lat", "lon", "alt_baro_ft", "on_ground", "gs_kt",
                "track_deg", "turn_deg", "squawk", "emergency", "pos_time", "is_airliner"]
ALERT_COLUMNS = ["alert_time", "alert_type", "flight", "hex", "alt_baro_ft", "detail"]
MOVEMENT_COLUMNS = ["window_start", "movement", "hex", "flight"]


def connect():
    """Open a session; rows come back as dictionaries, reads use LOCAL_QUORUM."""
    profile = ExecutionProfile(
        load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy(local_dc=LOCAL_DC)),
        consistency_level=ConsistencyLevel.LOCAL_QUORUM,
        request_timeout=10,
        row_factory=dict_factory,
    )
    cluster = Cluster(CASSANDRA_HOSTS, execution_profiles={EXEC_PROFILE_DEFAULT: profile})
    return cluster.connect(KEYSPACE)


def _days(start, end):
    """Calendar days (UTC) from start to end, so a window across midnight reads both days."""
    days, current = [], start.date()
    while current <= end.date():
        days.append(current)
        current += timedelta(days=1)
    return days


def _frame(rows, columns, time_columns):
    frame = pd.DataFrame(rows, columns=columns)
    for column in time_columns:
        frame[column] = pd.to_datetime(frame[column], utc=True)   # driver gives naive UTC times
    return frame


def batch_stats(session, since, now):
    """One row per processed batch since `since`: throughput, latency, data quality."""
    rows = []
    for day in _days(since, now):
        rows += session.execute(
            f"SELECT {', '.join(STATS_COLUMNS)} FROM batch_stats_by_day WHERE day = %s AND batch_time >= %s",
            (day, since),
        )
    return _frame(rows, STATS_COLUMNS, ["batch_time"])


def live_aircraft(session, now, max_age_s=90):
    """Newest position of every aircraft seen in the last `max_age_s` seconds."""
    rows = []
    current_minute = now.replace(second=0, microsecond=0)
    for back in range(3):       # this minute and the two before it
        minute = current_minute - timedelta(minutes=back)
        rows += session.execute(
            f"SELECT {', '.join(LIVE_COLUMNS)} FROM live_by_minute WHERE minute = %s", (minute,)
        )
    frame = _frame(rows, LIVE_COLUMNS, ["pos_time"])
    if frame.empty:
        return frame
    frame = frame.sort_values("pos_time").drop_duplicates("hex", keep="last")
    return frame[frame["pos_time"] >= now - timedelta(seconds=max_age_s)]


def alerts(session, now, hours=24):
    """Circling and emergency alerts of the last `hours`, newest first."""
    since = now - timedelta(hours=hours)
    rows = []
    for day in _days(since, now):
        rows += session.execute(
            f"SELECT {', '.join(ALERT_COLUMNS)} FROM alerts_by_day WHERE day = %s AND alert_time >= %s",
            (day, since),
        )
    return _frame(rows, ALERT_COLUMNS, ["alert_time"]).sort_values("alert_time", ascending=False)


def movements(session, now, hours=6):
    """Hamburg arrivals and departures of the last `hours`, one row per movement."""
    since = now - timedelta(hours=hours)
    rows = []
    for day in _days(since, now):
        rows += session.execute(
            f"SELECT {', '.join(MOVEMENT_COLUMNS)} FROM movements_by_day "
            "WHERE airport = %s AND day = %s AND window_start >= %s",
            (AIRPORT, day, since),
        )
    return _frame(rows, MOVEMENT_COLUMNS, ["window_start"])

"""Offline simulation of process_batch with a fake Kafka and a fake Cassandra.

Run: python -m pytest processor/tests   (needs the packages from processor/requirements.txt)
"""
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import processor as p  # noqa: E402

T0 = datetime(2026, 11, 12, 9, 0, 0, tzinfo=timezone.utc)
STATEMENTS = {k: k for k in ["position", "track", "recent_alerts", "alert", "movement", "live", "stats"]}


class FakeCassandra:
    """Keeps written rows in lists and answers the two read queries from them."""

    def __init__(self):
        self.rows = {k: [] for k in ["position", "alert", "movement", "live", "stats"]}

    def write_all(self, session, statement, rows):
        if statement == "track":
            return [[SimpleNamespace(pos_time=r[2], track_deg=r[12], on_ground=r[10], is_stale=r[17],
                                     squawk=r[14], emergency=r[15])
                     for r in self.rows["position"] if r[0] == h and r[1] == d and s <= r[2] <= e]
                    for h, d, s, e in rows]
        self.rows[statement].extend(rows)
        return [None] * len(rows)

    def execute(self, statement, params):
        day, since = params
        return [SimpleNamespace(hex=a[2], alert_type=a[3]) for a in self.rows["alert"] if a[0] == day and a[1] >= since]


class FakeDLQ:
    def __init__(self):
        self.sent = []

    def produce(self, topic, key, value):
        self.sent.append(json.loads(value)["reasons"])

    def flush(self, timeout):
        return 0


class Msg:
    def __init__(self, payload, offset, when=T0):
        self._value = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
        self._offset, self._ts = offset, int(when.timestamp() * 1000)

    def value(self): return self._value
    def key(self): return b"key"
    def partition(self): return 0
    def offset(self): return self._offset
    def timestamp(self): return (1, self._ts)
    def error(self): return None


@pytest.fixture
def db(monkeypatch):
    fake = FakeCassandra()
    monkeypatch.setattr(p, "write_all", fake.write_all)
    return fake


def position(i, **changes):
    record = {"hex": "4b1abc", "flight": "SWR1ABC ", "alt_baro": 8000, "gs": 220.0,
              "track": float((15 * i) % 360), "squawk": "1000", "emergency": "none", "category": "A3",
              "lat": 53.63, "lon": 9.39, "seen_pos": 0.4,
              "event_time": (T0 + timedelta(seconds=5 * i)).isoformat(), "source": "adsb.lol",
              "schema_version": 1, "ingested_at": (T0 + timedelta(seconds=5 * i + 1)).isoformat()}
    record.update(changes)
    return record


def test_bad_records_go_to_dlq_and_ground_vehicles_are_excluded(db):
    dlq = FakeDLQ()
    tug = {"hex": "3c80e1", "alt_baro": "ground", "category": "C0", "lat": 53.53, "lon": 9.83,
           "seen_pos": 1.0, "event_time": T0.isoformat()}
    batch = [Msg(position(0), 1), Msg(tug, 2), Msg(b"{broken", 3), Msg(position(0, hex="aaaaaa", lat=123.0), 4)]
    summary = p.process_batch(batch, db, STATEMENTS, dlq)
    assert summary == {"in": 4, "stored": 1, "dlq": 2, "excluded": 1, "alerts": 0, "movements": 0}
    assert dlq.sent == [["message:invalid JSON"], ["lat:in_range(-90, 90)"]]


def test_full_circle_and_emergency_each_alert_once(db):
    for i in range(31):     # 2.5 minutes of a standard-rate turn, squawk 7700 for 20 seconds
        squawk = "7700" if 3 <= i <= 6 else "1000"
        p.process_batch([Msg(position(i, squawk=squawk), i, T0 + timedelta(seconds=5 * i))], db, STATEMENTS, FakeDLQ())
    types = [alert[3] for alert in db.rows["alert"]]
    assert types.count("circling") == 1 and types.count("emergency") == 1
    assert len(db.rows["position"]) == 31 and len(db.rows["stats"]) == 31


def test_replaying_a_batch_changes_nothing_new(db):
    batch = [Msg(position(0), 7)]
    p.process_batch(batch, db, STATEMENTS, FakeDLQ())
    p.process_batch(batch, db, STATEMENTS, FakeDLQ())     # as after a crash before commit
    keys = {(r[0], r[1], r[2]) for r in db.rows["position"]}
    assert len(keys) == 1                                   # same primary key: Cassandra keeps one row


def test_departure_at_hamburg(db):
    for i, ground in enumerate([True, True, False, False]):
        record = position(i, hex="3c6abc", lat=53.625, lon=9.98, track=230.0,
                          alt_baro="ground" if ground else 1200)
        p.process_batch([Msg(record, 100 + i)], db, STATEMENTS, FakeDLQ())
    assert [m[4] for m in db.rows["movement"]] == ["departure"]


def test_helicopter_circling_does_not_alert(db):
    for i in range(31):
        record = position(i, hex="3e0198", flight="DHRUM", category="A7", t="EC35")
        p.process_batch([Msg(record, i, T0 + timedelta(seconds=5 * i))], db, STATEMENTS, FakeDLQ())
    assert [a for a in db.rows["alert"] if a[3] == "circling"] == []
    assert abs(db.rows["live"][-1][9]) >= 360       # still visible on the turn watchlist


def test_single_glitch_or_lifeguard_does_not_alert(db):
    for i in range(10):
        status = "unlawful" if i == 4 else ("lifeguard" if i >= 6 else "none")
        p.process_batch([Msg(position(i, emergency=status), i, T0 + timedelta(seconds=5 * i))], db, STATEMENTS, FakeDLQ())
    assert [a for a in db.rows["alert"] if a[3] == "emergency"] == []


def test_airliner_with_unknown_category_still_alerts(db):
    for i in range(31):     # DLH8C: an A321 broadcasting category A0
        record = position(i, hex="3c6648", flight="DLH8C", category="A0", t="A321")
        p.process_batch([Msg(record, i, T0 + timedelta(seconds=5 * i))], db, STATEMENTS, FakeDLQ())
    assert [a[3] for a in db.rows["alert"]] == ["circling"]
    assert db.rows["live"][-1][-1] is True          # is_airliner written to the live table

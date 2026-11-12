"""Checks data.py against rows shaped like the Cassandra driver's output (dicts, naive UTC times)."""
import importlib.util
from datetime import datetime, timedelta, timezone
from pathlib import Path

_spec = importlib.util.spec_from_file_location("dashboard_data", Path(__file__).resolve().parents[1] / "data.py")
data = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(data)

NOW = datetime(2026, 11, 13, 0, 0, 30, tzinfo=timezone.utc)       # just after midnight UTC


class FakeSession:
    def __init__(self, answers):
        self.answers, self.calls = answers, []

    def execute(self, query, params):
        self.calls.append(params)
        table = query.split(" FROM ")[1].split()[0]
        return self.answers.get(table, [])


def naive(moment):
    return moment.replace(tzinfo=None)


def test_live_keeps_newest_position_per_aircraft_and_drops_old_ones():
    rows = [
        {"hex": "aaa111", "pos_time": naive(NOW - timedelta(seconds=40)), "flight": "OLD", "on_ground": False},
        {"hex": "aaa111", "pos_time": naive(NOW - timedelta(seconds=5)), "flight": "NEW", "on_ground": False},
        {"hex": "bbb222", "pos_time": naive(NOW - timedelta(seconds=300)), "flight": "GONE", "on_ground": False},
    ]
    session = FakeSession({"live_by_minute": rows})
    frame = data.live_aircraft(session, NOW)
    assert list(frame["flight"]) == ["NEW"]
    assert str(frame["pos_time"].dt.tz) == "UTC"
    assert len(session.calls) == 3                      # this minute and the two before


def test_windows_across_midnight_read_both_days():
    session = FakeSession({"batch_stats_by_day": []})
    data.batch_stats(session, NOW - timedelta(minutes=30), NOW)
    assert [call[0].isoformat() for call in session.calls] == ["2026-11-12", "2026-11-13"]


def test_empty_results_keep_their_columns():
    session = FakeSession({})
    assert list(data.alerts(session, NOW).columns) == data.ALERT_COLUMNS
    assert data.movements(session, NOW).empty

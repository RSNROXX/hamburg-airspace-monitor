"""Renders the dashboard with fake data in four situations. Run: python -m pytest dashboard/tests"""
import sys
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path

import importlib.util

import pandas as pd
from streamlit.testing.v1 import AppTest

APP = str(Path(__file__).resolve().parents[1] / "app.py")
_spec = importlib.util.spec_from_file_location("dashboard_data", Path(APP).with_name("data.py"))
real_data = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(real_data)       # the real module, only to read its column lists
NOW = datetime.now(timezone.utc)


def stats_rows(age_s=2, minutes=30):
    times = [NOW - timedelta(seconds=age_s + 60 * m) for m in range(minutes)]
    return pd.DataFrame({
        "batch_time": pd.to_datetime(times, utc=True), "records_in": 400, "records_valid": 398,
        "records_dlq": 2, "records_excluded": 10, "records_stale": 3, "duplicates": 40,
        "missing_track": 5, "missing_speed": 4, "missing_altitude": 1, "implausible": 1,
        "latency_p50_ms": 2500.0, "latency_p95_ms": 9000.0})


def live_rows():
    return pd.DataFrame({
        "hex": ["4b1abc", "3e0198", "3c6abc"], "flight": ["SWR1ABC", "DHRUM", None],
        "category": ["A3", "A7", "A5"], "lat": [53.6, 53.5, 52.0], "lon": [9.4, 9.9, 8.0],
        "alt_baro_ft": [8000, 1200, 35000], "on_ground": [False, False, False],
        "gs_kt": [220.0, 90.0, 450.0], "track_deg": [10.0, 200.0, 90.0],
        "turn_deg": [250.0, -400.0, 3.0], "squawk": ["1000", "0020", "7700"],
        "emergency": ["none", "none", "none"],
        "pos_time": pd.to_datetime([NOW - timedelta(seconds=5)] * 3, utc=True),
        "is_airliner": [True, False, True]})


def alert_rows():
    return pd.DataFrame({
        "alert_time": pd.to_datetime([NOW - timedelta(minutes=3)], utc=True), "alert_type": ["circling"],
        "flight": ["SWR1ABC"], "hex": ["4b1abc"], "alt_baro_ft": [8000],
        "detail": ["turned 372 degrees to the right within 10 minutes"]})


def movement_rows():
    windows = pd.to_datetime([NOW.replace(minute=0, second=0, microsecond=0)] * 3, utc=True)
    return pd.DataFrame({"window_start": windows, "movement": ["arrival", "arrival", "departure"],
                         "hex": ["a", "b", "c"], "flight": ["X1", "X2", "X3"]})


def empty(columns, times):
    frame = pd.DataFrame({c: [] for c in columns})
    for column in times:
        frame[column] = pd.to_datetime(frame[column], utc=True)
    return frame


def fake_data(stats, live, alerts_, moves, fail=False):
    module = types.ModuleType("data")
    module.connect = lambda: object()
    def guard(value):
        def reader(*args, **kwargs):
            if fail:
                raise ConnectionError("all Cassandra nodes are down")
            return value
        return reader
    module.batch_stats, module.live_aircraft = guard(stats), guard(live)
    module.alerts, module.movements = guard(alerts_), guard(moves)
    return module


def run(module, monkeypatch):
    monkeypatch.setitem(sys.modules, "data", module)
    app = AppTest.from_file(APP, default_timeout=30)
    app.run()
    assert not app.exception, app.exception
    return app


def test_normal_view(monkeypatch):
    app = run(fake_data(stats_rows(), live_rows(), alert_rows(), movement_rows()), monkeypatch)
    assert "Receiving live data" in app.success[0].value
    metrics = {m.label: m.value for m in app.metric}
    assert metrics["Aircraft airborne now"] == "3"
    assert metrics["Alerts (24 h)"] == "1"
    watchlist = app.dataframe[0].value
    assert list(watchlist["Flight"])[:2] == ["DHRUM", "SWR1ABC"]          # sorted by size of turn
    assert list(watchlist["Type"]) == ["Other", "Airliner", "Emergency"]


def test_empty_database(monkeypatch):
    d = real_data
    app = run(fake_data(empty(d.STATS_COLUMNS, ["batch_time"]), empty(d.LIVE_COLUMNS, ["pos_time"]),
                        empty(d.ALERT_COLUMNS, ["alert_time"]), empty(d.MOVEMENT_COLUMNS, ["window_start"])),
              monkeypatch)
    assert "No data received" in app.error[0].value


def test_stale_data_is_flagged(monkeypatch):
    app = run(fake_data(stats_rows(age_s=120), live_rows(), alert_rows(), movement_rows()), monkeypatch)
    assert "No new data since" in app.error[0].value


def test_cassandra_down_shows_message(monkeypatch):
    app = run(fake_data(None, None, None, None, fail=True), monkeypatch)
    assert "Cannot read from Cassandra" in app.error[0].value

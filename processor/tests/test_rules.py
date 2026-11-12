"""Offline tests for rules.py. Run: python -m pytest processor/tests"""
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import rules  # noqa: E402

T0 = datetime(2026, 11, 11, 12, 0, 0, tzinfo=timezone.utc)

# Two records taken from the real sample of 7 Nov 2026, plus producer metadata.
AIRBUS = {"hex": "395d66", "flight": "BGA431G ", "r": "F-GXLG", "t": "A337", "alt_baro": "ground",
          "gs": 0.1, "squawk": "2000", "category": "A5", "lat": 53.536114, "lon": 9.828958,
          "seen_pos": 15.092, "event_time": "2026-11-07T16:20:51.408000+00:00",
          "source": "adsb.lol", "schema_version": 1}
TUG = {"hex": "3c80e1", "flight": "TXLU00  ", "alt_baro": "ground", "category": "C0",
       "lat": 53.530314, "lon": 9.836136, "event_time": None}


def test_normalize_real_sample():
    r = rules.normalize(AIRBUS)
    assert r["flight"] == "BGA431G"                    # trailing space removed
    assert r["on_ground"] is True and r["alt_baro_ft"] is None
    assert r["pos_time"] == datetime(2026, 11, 7, 16, 20, 51, tzinfo=timezone.utc)   # rounded
    assert r["is_ground_vehicle"] is False
    assert rules.normalize(TUG)["is_ground_vehicle"] is True


def test_rounding_makes_repeats_identical():
    a = rules.normalize({**AIRBUS, "event_time": "2026-11-07T16:20:51.408000+00:00"})
    b = rules.normalize({**AIRBUS, "event_time": "2026-11-07T16:20:51.409000+00:00"})
    assert a["pos_time"] == b["pos_time"]


def test_validate_catches_bad_records():
    good = rules.normalize(AIRBUS)
    no_position = rules.normalize(TUG)                      # event_time missing
    bad_lat = rules.normalize({**AIRBUS, "lat": 123.0})
    bad_hex = rules.normalize({**AIRBUS, "hex": "XYZ"})
    too_fast = rules.normalize({**AIRBUS, "alt_baro": 35000, "gs": 1500})
    valid, reasons = rules.validate([good, no_position, bad_lat, bad_hex, too_fast])
    assert valid == [0]
    assert any(x.startswith("pos_time") for x in reasons[1])
    assert any(x.startswith("lat") for x in reasons[2])
    assert any(x.startswith("hex") for x in reasons[3])
    assert rules.is_implausible(reasons[4]) and not rules.is_implausible(reasons[2])


def test_validate_all_good_and_empty():
    assert rules.validate([rules.normalize(AIRBUS)] * 3) == ([0, 1, 2], {})
    assert rules.validate([]) == ([], {})


def test_signed_turn_wraps_around_north():
    assert rules.signed_turn(350, 10) == 20
    assert rules.signed_turn(10, 350) == -20
    assert rules.signed_turn(90, 270) == -180


def test_full_circle_is_detected():
    # A standard-rate turn: 3 degrees per second, sampled every 5 seconds for 2.5 minutes.
    samples = [(T0 + timedelta(seconds=5 * i), (15 * i) % 360) for i in range(31)]
    assert rules.total_turn(samples) >= 360


def test_s_turns_cancel_out():
    tracks = [0, 30, 60, 30, 0, 30, 60, 30, 0]
    samples = [(T0 + timedelta(seconds=5 * i), t) for i, t in enumerate(tracks)]
    assert abs(rules.total_turn(samples)) < 90


def test_gaps_are_not_counted():
    samples = [(T0, 0), (T0 + timedelta(seconds=120), 170)]   # 2-minute gap
    assert rules.total_turn(samples) == 0


def test_emergency_rules():
    assert rules.emergency_reason("7700", "none") == "squawk 7700 (general emergency)"
    assert rules.emergency_reason("2000", "minfuel") == "emergency status: minfuel"
    assert rules.emergency_reason("2000", "none") is None
    assert rules.emergency_reason(None, None) is None
    assert rules.emergency_reason("1000", "lifeguard") is None      # medical priority, not distress


def test_movement_and_distance():
    assert rules.movement(False, True) == "arrival"
    assert rules.movement(True, False) == "departure"
    assert rules.movement(True, True) is None
    assert rules.distance_km(rules.HAM_LAT, rules.HAM_LON, rules.HAM_LAT, rules.HAM_LON) == 0
    assert 9 < rules.distance_km(53.6304, 9.9882, 53.55, 9.90) < 12   # airport to city centre


def test_windows_and_days():
    assert rules.window_start(T0.replace(minute=44, second=10)) == T0.replace(minute=30)
    before_midnight = datetime(2026, 11, 11, 23, 55, tzinfo=timezone.utc)
    assert len(rules.days_between(before_midnight, before_midnight + timedelta(minutes=10))) == 2


def test_percentile():
    values = list(range(1, 101))            # 1..100
    assert rules.percentile(values, 0.5) == 50
    assert rules.percentile(values, 0.95) == 95
    assert rules.percentile(values, 1.0) == 100
    assert rules.percentile([], 0.5) is None


def test_only_airliners_count_for_circling():
    assert rules.counts_for_circling("A3") and rules.counts_for_circling("A5")      # no type known
    assert not rules.counts_for_circling("A1")      # light aircraft
    assert not rules.counts_for_circling("A7")      # helicopter
    assert not rules.counts_for_circling(None)      # nothing known


def test_aircraft_type_beats_a_wrong_category():
    assert rules.is_airliner("A0", "A321")          # real case: DLH8C, an A321 sending A0
    assert rules.is_airliner(None, "B38M")          # 737 MAX 8
    assert not rules.is_airliner("A3", "C172")      # a Cessna claiming to be large
    assert not rules.is_airliner("A2", "B350")      # King Air 350 is not a 737


def test_emergency_needs_confirmation():
    def at(seconds, squawk="1000", status="none"):
        return (T0 + timedelta(seconds=seconds), squawk, status)
    now = T0 + timedelta(seconds=20)
    glitch = [at(0), at(5), at(10, status="unlawful"), at(15), at(20)]
    real = [at(0), at(5, "7700"), at(10, "7700"), at(15, "7700"), at(20, "7700")]
    assert rules.confirmed_emergency(glitch, now) is None
    assert rules.confirmed_emergency(real, now) == "squawk 7700 (general emergency)"
    old_only = [at(-300, "7700"), at(-295, "7700"), at(-290, "7700"), at(20)]
    assert rules.confirmed_emergency(old_only, now) is None          # outside the 2-minute window

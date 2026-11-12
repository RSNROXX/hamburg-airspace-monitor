"""Processing rules: pure Python, no Kafka and no Cassandra.

Everything here takes plain values and returns plain values, so it can be
tested offline (see tests/test_rules.py).
"""

import math
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import pandera.pandas as pa

# --- Constants ---------------------------------------------------------------
HAM_LAT, HAM_LON = 53.6304, 9.9882          # Hamburg Airport reference point
EMERGENCY_SQUAWKS = {
    "7500": "unlawful interference",
    "7600": "radio failure",
    "7700": "general emergency",
}
# ADS-B emitter categories that holding for congestion applies to: A2 small, A3 large
# (e.g. A320), A4 high-vortex large (e.g. B757), A5 heavy (e.g. A350). Excluded: A1 light
# aircraft, A7 helicopters, B gliders/balloons, and unknown: they circle for training,
# police or sightseeing work, which says nothing about airport congestion.
CIRCLING_CATEGORIES = {"A2", "A3", "A4", "A5"}
MAX_GAP_S = 30          # turns across a gap longer than this are not counted
STALE_AFTER_S = 60      # a position older than this when received is "stale"


# --- 1. Normalize: raw message -> clean, typed record --------------------------
def _number(value):
    """Return value as a float, or None if it is missing or not a number."""
    if isinstance(value, bool):             # True/False are not measurements
        return None
    if isinstance(value, (int, float)) and math.isfinite(value):
        return float(value)
    return None


def _text(value):
    """Return trimmed text, or None if missing or empty."""
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _round_to_second(moment):
    """Round a datetime to the nearest whole second.

    The same position seen in two polls can differ by a millisecond after
    the event-time calculation; rounding makes both copies identical, so the
    Cassandra primary key treats them as one row.
    """
    rounded = moment.replace(microsecond=0)
    if moment.microsecond >= 500_000:
        rounded += timedelta(seconds=1)
    return rounded


def normalize(raw):
    """Turn one raw producer message (a dict) into a clean record (a dict)."""
    alt_baro = raw.get("alt_baro")
    on_ground = alt_baro == "ground"
    altitude = None if on_ground else _number(alt_baro)

    event_time = None
    if isinstance(raw.get("event_time"), str):
        try:
            event_time = datetime.fromisoformat(raw["event_time"]).astimezone(timezone.utc)
        except ValueError:
            event_time = None

    hex_id = _text(raw.get("hex"))
    category = _text(raw.get("category"))
    seen_pos = _number(raw.get("seen_pos"))

    return {
        "hex": hex_id.lower() if hex_id else None,
        "flight": _text(raw.get("flight")),
        "registration": _text(raw.get("r")),
        "aircraft_type": _text(raw.get("t")),
        "category": category,
        "lat": _number(raw.get("lat")),
        "lon": _number(raw.get("lon")),
        "alt_baro_ft": altitude,
        "on_ground": on_ground,
        "gs_kt": _number(raw.get("gs")),
        "track_deg": _number(raw.get("track")),
        "true_heading_deg": _number(raw.get("true_heading")),
        "squawk": _text(raw.get("squawk")),
        "emergency": _text(raw.get("emergency")),
        "seen_pos_s": seen_pos,
        "is_stale": seen_pos is not None and seen_pos > STALE_AFTER_S,
        "pos_time": _round_to_second(event_time) if event_time else None,
        "source": _text(raw.get("source")),
        "schema_version": raw.get("schema_version"),
        "ingested_at": raw.get("ingested_at"),
        "is_ground_vehicle": bool(category and category.startswith("C")),
    }


# --- 2. Validate: Pandera schema on a batch of records ------------------------
SCHEMA = pa.DataFrameSchema(
    {
        # Required fields: a record without them cannot be used at all.
        "hex": pa.Column(object, pa.Check(lambda s: s.str.match(r"^~?[0-9a-f]{6}$")), nullable=False),
        "lat": pa.Column(float, pa.Check.in_range(-90, 90), nullable=False),
        "lon": pa.Column(float, pa.Check.in_range(-180, 180), nullable=False),
        "pos_time": pa.Column(object, nullable=False),
        # Optional fields: may be missing, but if present must be plausible.
        "gs_kt": pa.Column(float, pa.Check.in_range(0, 1000), nullable=True),
        "alt_baro_ft": pa.Column(float, pa.Check.in_range(-2000, 60000), nullable=True),
        "track_deg": pa.Column(float, pa.Check.in_range(0, 360), nullable=True),
    },
    strict=False,   # other columns are allowed and not checked
)
PLAUSIBILITY_COLUMNS = {"gs_kt", "alt_baro_ft"}


def validate(records):
    """Check a list of normalized records.

    Returns (valid_positions, reasons): the list positions that passed, and
    for each failed position a list of reasons like "lat:in_range(-90, 90)".
    """
    if not records:
        return [], {}
    columns = list(SCHEMA.columns)
    frame = pd.DataFrame([{c: r[c] for c in columns} for r in records], dtype=object)
    for column in ("lat", "lon", "gs_kt", "alt_baro_ft", "track_deg"):
        frame[column] = pd.to_numeric(frame[column]).astype(float)

    reasons = {}
    try:
        SCHEMA.validate(frame, lazy=True)       # lazy = collect all failures, not only the first
    except pa.errors.SchemaErrors as errors:
        for _, failure in errors.failure_cases.iterrows():
            position = failure["index"]
            if position is None or pd.isna(position):
                continue
            reasons.setdefault(int(position), []).append(f"{failure['column']}:{failure['check']}")

    valid = [i for i in range(len(records)) if i not in reasons]
    return valid, reasons


def is_implausible(reason_list):
    """True if a record failed only because of an implausible value (accuracy, not structure)."""
    return any(reason.split(":")[0] in PLAUSIBILITY_COLUMNS for reason in reason_list)


# --- 3. Detection rules ---------------------------------------------------------
def signed_turn(from_deg, to_deg):
    """Smallest signed change from one direction to another, between -180 and +180.

    Example: from 350 to 10 is +20 (a right turn), not -340.
    """
    return (to_deg - from_deg + 540) % 360 - 180


def total_turn(samples):
    """Add up the signed turns along a track.

    samples: list of (pos_time, track_deg), oldest first.
    Left and right turns cancel out, so S-turns do not add up to a circle.
    Turns across gaps longer than MAX_GAP_S are skipped, because we cannot
    know which way the aircraft turned while we did not see it.
    """
    total = 0.0
    previous = None
    for moment, track in samples:
        if track is None:
            continue
        if previous is not None:
            gap = (moment - previous[0]).total_seconds()
            if 0 < gap <= MAX_GAP_S:
                total += signed_turn(previous[1], track)
        previous = (moment, track)
    return total


# "lifeguard" marks a medical priority flight (e.g. an organ transport), not distress.
NON_EMERGENCY_STATUSES = {"none", "lifeguard"}


def emergency_reason(squawk, emergency):
    """Return a short description if the aircraft signals an emergency, else None."""
    if emergency and emergency.lower() not in NON_EMERGENCY_STATUSES:
        return f"emergency status: {emergency}"
    if squawk in EMERGENCY_SQUAWKS:
        return f"squawk {squawk} ({EMERGENCY_SQUAWKS[squawk]})"
    return None


def distance_km(lat1, lon1, lat2, lon2):
    """Great-circle distance between two points (haversine formula)."""
    earth_radius_km = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * earth_radius_km * math.asin(math.sqrt(a))


def movement(previous_on_ground, current_on_ground):
    """'arrival' (air -> ground), 'departure' (ground -> air), or None."""
    if previous_on_ground is False and current_on_ground is True:
        return "arrival"
    if previous_on_ground is True and current_on_ground is False:
        return "departure"
    return None


def window_start(moment, minutes=15):
    """Start of the fixed time window that contains `moment` (tumbling window)."""
    floored_minute = moment.minute - moment.minute % minutes
    return moment.replace(minute=floored_minute, second=0, microsecond=0)


def days_between(start, end):
    """All calendar days (UTC) from start to end, inclusive. Used across midnight."""
    days, current = [], start.date()
    while current <= end.date():
        days.append(current)
        current += timedelta(days=1)
    return days


# --- 4. Statistics ----------------------------------------------------------------
def percentile(values, share):
    """Value below which `share` (0..1) of the values fall, e.g. 0.95 for the 95th percentile.

    Uses the "nearest rank" method: sort, then pick the value at that rank.
    Returns None for an empty list.
    """
    if not values:
        return None
    ordered = sorted(values)
    rank = max(1, math.ceil(share * len(ordered)))
    return ordered[rank - 1]


# ICAO aircraft type designators of airliners (prefix match): Airbus A220 to A380,
# Boeing 717 to 787 and 737 MAX, Embraer E-Jets, Bombardier CRJ, ATR, Dash 8, and a few others.
AIRLINER_TYPE_PREFIXES = (
    "A30", "A31", "A32", "A33", "A34", "A35", "A38", "A20N", "A21N", "A221", "A223", "BCS",
    "B71", "B72", "B73", "B37M", "B38M", "B39M", "B3XM", "B74", "B75", "B76", "B77", "B78",
    "E17", "E19", "E29", "E75", "CRJ", "AT4", "AT7", "DH8", "SU95", "C919", "MD8", "MD9",
    "F70", "F100", "B46", "RJ8",
)


def is_airliner(category, aircraft_type):
    """Decide whether an aircraft is an airliner.

    The aircraft type (e.g. "A321") comes from adsb.lol's aircraft database, looked up by the
    aircraft's ID, so it is used first. The emitter category is set by hand in each
    transponder and is sometimes wrong or "A0" (unknown), so it is only the fallback.
    Real examples: DLH8C is an A321 broadcasting category A0.
    """
    if aircraft_type:
        return aircraft_type.upper().startswith(AIRLINER_TYPE_PREFIXES)
    return category in CIRCLING_CATEGORIES


def counts_for_circling(category, aircraft_type=None):
    """True if a circle by this aircraft can signal congestion (only airliners)."""
    return is_airliner(category, aircraft_type)


def confirmed_emergency(samples, now, window_s=120, minimum=3):
    """Return the emergency reason only if it appears in at least `minimum` positions
    within the last `window_s` seconds, else None.

    samples: list of (pos_time, squawk, emergency).
    ADS-B is a radio signal and single messages are sometimes decoded wrongly; a real
    emergency stays set for minutes, a decoding glitch appears once.
    """
    reasons = [
        emergency_reason(squawk, emergency)
        for moment, squawk, emergency in samples
        if 0 <= (now - moment).total_seconds() <= window_s
    ]
    for reason in set(r for r in reasons if r):
        if reasons.count(reason) >= minimum:
            return reason
    return None

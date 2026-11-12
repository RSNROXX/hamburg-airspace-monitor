"""Hamburg Airspace Monitor: Streamlit dashboard.

Reads only from Cassandra (never from Kafka), so it can be restarted at any time
without affecting the pipeline. The live part refreshes itself every REFRESH_S seconds.
"""

import os
from datetime import datetime, timedelta, timezone

import pandas as pd
import streamlit as st

import data

REFRESH_S = int(os.getenv("REFRESH_S", "10"))
LOCAL_TZ = "Europe/Berlin"
STALE_AFTER_S = 30                       # no new batch for this long = "no new data"
COLOURS = {"Airliner": "#2563eb", "Other": "#9ca3af", "Emergency": "#dc2626"}

st.set_page_config(page_title="Hamburg Airspace Monitor", layout="wide")


@st.cache_resource
def session():
    """One Cassandra connection for the whole app, reused on every refresh."""
    return data.connect()


def local(series):
    """UTC times -> local wall-clock times for display."""
    return series.dt.tz_convert(LOCAL_TZ).dt.strftime("%H:%M:%S")


def aircraft_kind(row):
    if (row["emergency"] not in (None, "none") and pd.notna(row["emergency"])) or row["squawk"] in ("7500", "7600", "7700"):
        return "Emergency"
    return "Airliner" if row["is_airliner"] is True else "Other"     # decided by the processor


def show_status(stats, now):
    if stats.empty:
        st.error("No data received in the last 30 minutes. Check the producer and processor logs.")
        return
    last = stats["batch_time"].max()
    age_s = (now - last).total_seconds()
    when = last.tz_convert(LOCAL_TZ).strftime("%H:%M:%S")
    if age_s > STALE_AFTER_S:
        st.error(f"No new data since {when} ({age_s:.0f} s ago). Source, producer or processor may be down.")
    else:
        st.success(f"Receiving live data. Last batch at {when} ({age_s:.0f} s ago).")


def show_kpis(stats, live, alert_rows, now):
    last_minute = stats[stats["batch_time"] >= now - timedelta(seconds=60)]
    last_5_min = stats[stats["batch_time"] >= now - timedelta(minutes=5)]
    airborne = live[~live["on_ground"].astype(bool)] if not live.empty else live
    columns = st.columns(5)
    columns[0].metric("Aircraft airborne now", len(airborne))
    columns[1].metric("Messages per second", f"{last_minute['records_in'].sum() / 60:.1f}")
    columns[2].metric("Latency p50 (5 min)",
                      f"{last_5_min['latency_p50_ms'].median() / 1000:.1f} s" if not last_5_min.empty else "n/a")
    columns[3].metric("Latency p95 (5 min)",
                      f"{last_5_min['latency_p95_ms'].median() / 1000:.1f} s" if not last_5_min.empty else "n/a")
    columns[4].metric("Alerts (24 h)", len(alert_rows))


def show_map(live):
    st.subheader("Live traffic")
    if live.empty:
        st.info("No aircraft seen in the last 90 seconds.")
        return
    points = live.copy()
    points["kind"] = points.apply(aircraft_kind, axis=1)
    points["colour"] = points["kind"].map(COLOURS)
    st.map(points, latitude="lat", longitude="lon", color="colour", size=1500, zoom=5)
    st.caption("Blue: airliner (by aircraft type, or category A2 to A5 if the type is unknown). "
               "Grey: other traffic. Red: emergency squawk.")


def show_watchlist(live):
    st.subheader("Airliners closest to a circling alert")
    st.caption("Airliners turning the most over the last 10 minutes. An alert fires at a full 360 degrees. "
               "Helicopters and light aircraft circle as part of their work and are left out on purpose.")
    if live.empty:
        st.info("No aircraft to show yet.")
        return
    airliners = live["is_airliner"].fillna(False).astype(bool)
    turning = live[airliners & ~live["on_ground"].astype(bool)].copy()
    if turning.empty:
        st.info("No airliners in the air right now.")
        return
    turning["abs_turn"] = turning["turn_deg"].abs()
    turning = turning.sort_values("abs_turn", ascending=False).head(10)
    table = pd.DataFrame({
        "Flight": turning["flight"].fillna(turning["hex"]),
        "Turn": turning["turn_deg"].map(lambda t: f"{abs(t):.0f}° {'right' if t > 0 else 'left'}"),
        "Towards a full circle": (turning["abs_turn"] / 360).clip(upper=1.0),
        "Altitude (ft)": turning["alt_baro_ft"],
        "Speed (kt)": turning["gs_kt"].round(0),
    })
    st.dataframe(table, hide_index=True, column_config={
        "Towards a full circle": st.column_config.ProgressColumn(min_value=0.0, max_value=1.0, format="percent"),
    })


def show_alerts(alert_rows):
    st.subheader("Alerts (last 24 hours)")
    if alert_rows.empty:
        st.info("No circling or emergency alerts in the last 24 hours.")
        return
    st.dataframe(pd.DataFrame({
        "Time": local(alert_rows["alert_time"]),
        "Type": alert_rows["alert_type"],
        "Flight": alert_rows["flight"].fillna(alert_rows["hex"]),
        "Altitude (ft)": alert_rows["alt_baro_ft"],
        "Detail": alert_rows["detail"],
    }), hide_index=True)


def show_pulse(movement_rows):
    st.subheader("Hamburg traffic pulse (per 15 minutes)")
    if movement_rows.empty:
        st.info("No arrivals or departures detected at HAM in the last 6 hours.")
        return
    counts = (movement_rows.groupby(["window_start", "movement"]).size()
              .unstack(fill_value=0).reset_index())
    counts["Window"] = counts["window_start"].dt.tz_convert(LOCAL_TZ).dt.strftime("%H:%M")
    series = [column for column in ("arrival", "departure") if column in counts]
    st.bar_chart(counts, x="Window", y=series, y_label="Movements")


def show_data_quality(stats, now):
    st.subheader("Data quality (last 15 minutes)")
    window = stats[stats["batch_time"] >= now - timedelta(minutes=15)]
    total = int(window["records_in"].sum())
    if total == 0:
        st.info("No records in the last 15 minutes.")
        return
    valid = int(window["records_valid"].sum())
    stored = valid - int(window["records_excluded"].sum())
    stored_share = (lambda n: f"{100 * n / stored:.1f} %") if stored else (lambda n: "n/a")
    st.dataframe(pd.DataFrame([
        ("Validity", "Records passing the schema", f"{100 * valid / total:.2f} %"),
        ("Validity", "Sent to dead-letter topic", int(window["records_dlq"].sum())),
        ("Accuracy", "Implausible values (speed or altitude)", int(window["implausible"].sum())),
        ("Completeness", "Speed missing", stored_share(int(window["missing_speed"].sum()))),
        ("Completeness", "Track missing (airborne)", int(window["missing_track"].sum())),
        ("Completeness", "Altitude missing (airborne)", int(window["missing_altitude"].sum())),
        ("Uniqueness", "Repeated positions (merged by primary key)", stored_share(int(window["duplicates"].sum()))),
        ("Timeliness", "Stale positions (older than 60 s)", stored_share(int(window["records_stale"].sum()))),
        ("Scope", "Ground vehicles excluded", int(window["records_excluded"].sum())),
    ], columns=["Dimension", "Check", "Value"]).astype({"Value": str}), hide_index=True)


def show_throughput(stats):
    st.subheader("Throughput and latency (last 30 minutes)")
    if stats.empty:
        st.info("No batches yet.")
        return
    # Only whole minutes: the oldest minute is cut by the 30-minute window, the newest is still running.
    first_full = stats["batch_time"].min().ceil("1min")
    current = pd.Timestamp.now(tz="UTC").floor("1min")
    finished = stats[(stats["batch_time"] >= first_full) & (stats["batch_time"] < current)]
    per_minute = finished.set_index("batch_time").resample("1min").agg(
        {"records_in": "sum", "latency_p95_ms": "median"}).reset_index()
    per_minute["Minute"] = per_minute["batch_time"].dt.tz_convert(LOCAL_TZ).dt.strftime("%H:%M")
    per_minute["Messages per minute"] = per_minute["records_in"]
    per_minute["Latency p95 (s)"] = per_minute["latency_p95_ms"] / 1000
    left, right = st.columns(2)
    left.line_chart(per_minute, x="Minute", y="Messages per minute")
    right.line_chart(per_minute, x="Minute", y="Latency p95 (s)")
    st.caption("Latency = time stored minus the time the aircraft was at that position, "
               "so it includes how old the position already was when the API returned it.")


st.title("Hamburg Airspace Monitor")
st.caption(f"Live ADS-B traffic within 250 nautical miles of Hamburg Airport. Refreshes every {REFRESH_S} s.")


@st.fragment(run_every=REFRESH_S)
def live_view():
    now = datetime.now(timezone.utc)
    try:
        cassandra = session()
        stats = data.batch_stats(cassandra, now - timedelta(minutes=30), now)
        live = data.live_aircraft(cassandra, now)
        alert_rows = data.alerts(cassandra, now)
        movement_rows = data.movements(cassandra, now)
    except Exception as exc:          # Cassandra unreachable: say so instead of crashing the page
        st.error(f"Cannot read from Cassandra right now: {type(exc).__name__}: {exc}")
        return

    show_status(stats, now)
    show_kpis(stats, live, alert_rows, now)
    left, right = st.columns([3, 2])
    with left:
        show_map(live)
    with right:
        show_watchlist(live)
    show_alerts(alert_rows)
    left, right = st.columns(2)
    with left:
        show_pulse(movement_rows)
    with right:
        show_data_quality(stats, now)
    show_throughput(stats)


live_view()

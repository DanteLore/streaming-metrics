import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from datetime import datetime, timezone

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from shared import pulsar_streams as streams
from shared.schema import (
    COMPLIANCE_CRITICAL_THRESHOLD,
    DEVICES,
    FOOD_SAFETY_MIN_TEMP,
    METRICS_TOPIC,
    TELEMETRY_TOPIC,
    VIBRATION_ALERT_RATIO,
)

st.set_page_config(
    page_title="Verdant Veggie Sausage Co.",
    page_icon="\U0001f331",
    layout="wide",
)

st_autorefresh(interval=2000, key="data_refresh")

LAG_FLAG = Path("data/flags/simulate_lag")

STAGE_COLOURS = {
    "ok":      "#00cc66",
    "warning": "#ffaa00",
    "alarm":   "#ff4444",
    "idle":    "#555577",
}
BG = "#0e1117"


# ── Stream reader session management ─────────────────────────────────────────

BUFFER_MAX = 500


def _init_readers() -> None:
    if "readers_ready" in st.session_state:
        return
    try:
        client = streams.make_client()
        st.session_state.telemetry_reader = streams.make_reader(client, TELEMETRY_TOPIC)
        st.session_state.metrics_reader = streams.make_reader(client, METRICS_TOPIC)
        st.session_state.telemetry_buffer = []
        st.session_state.metrics_buffer = []
        st.session_state.readers_ready = True
    except Exception:
        st.session_state.readers_ready = False


def _drain_readers() -> None:
    if not st.session_state.get("readers_ready"):
        return
    try:
        for reader_key, buffer_key in [
            ("telemetry_reader", "telemetry_buffer"),
            ("metrics_reader",   "metrics_buffer"),
        ]:
            fresh = streams.drain(st.session_state[reader_key])
            buf = st.session_state.get(buffer_key, [])
            st.session_state[buffer_key] = (fresh + buf)[:BUFFER_MAX]
    except Exception:
        st.session_state.pop("readers_ready", None)


_init_readers()
_drain_readers()

t_buf = st.session_state.get("telemetry_buffer", [])
m_buf = st.session_state.get("metrics_buffer", [])


# ── Buffer query helpers ──────────────────────────────────────────────────────

def latest_sensor_values(buf: list) -> pd.DataFrame:
    seen: dict = {}
    for msg in buf:  # buf is newest-first
        did = msg.get("device_id")
        if did and did not in seen:
            seen[did] = msg
    return pd.DataFrame(list(seen.values())) if seen else pd.DataFrame()


def data_lag_seconds(buf: list) -> float | None:
    if not buf:
        return None
    try:
        ts = datetime.fromisoformat(buf[0]["timestamp"])
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return (datetime.now(tz=timezone.utc) - ts).total_seconds()
    except Exception:
        return None


def _filter_metric(buf: list, metric_type: str, n: int = 200) -> pd.DataFrame:
    rows = [m for m in buf if m.get("metric") == metric_type][:n]
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def _dedup(df: pd.DataFrame, key: str) -> pd.DataFrame:
    """Keep the latest computed result per window (buffer is newest-first)."""
    return df.drop_duplicates(subset=[key], keep="first") if not df.empty else df


def temp_window_metrics(buf: list) -> pd.DataFrame:
    df = _filter_metric(buf, "temp_window")
    if not df.empty:
        df = _dedup(df, "window_start")
        df["mean_temp"] = df["temp_sum"] / df["temp_count"]
        df = df.sort_values("window_start")
    return df


def production_hourly(buf: list) -> pd.DataFrame:
    df = _filter_metric(buf, "production_hourly")
    if not df.empty:
        df = _dedup(df, "hour")
        df = df.sort_values("hour")
    return df


def compliance_violations(buf: list) -> pd.DataFrame:
    df = _filter_metric(buf, "compliance_window")
    if not df.empty:
        df = _dedup(df, "window_start")
        df = df[df["mean_temp"] < FOOD_SAFETY_MIN_TEMP]
        df = df.sort_values("window_start", ascending=False)
    return df


def vibration_window_metrics(buf: list) -> pd.DataFrame:
    df = _filter_metric(buf, "vibration_window")
    if not df.empty:
        df = _dedup(df, "window_start")
        df = df.sort_values("window_start")
    return df


# ── Sensor status ─────────────────────────────────────────────────────────────

def sensor_status(device_id: str, value: float) -> str:
    d = DEVICES[device_id]
    lo, hi = d["min"], d["max"]
    if value < lo or value > hi:
        return "alarm"
    margin = (hi - lo) * 0.1
    if value < lo + margin or value > hi - margin:
        return "warning"
    return "ok"


# ── SCADA diagram ─────────────────────────────────────────────────────────────

def build_scada(readings: pd.DataFrame) -> go.Figure:
    vals = dict(zip(readings["device_id"], readings["value"])) if not readings.empty else {}

    stages = [
        ("HOPPER",   8,  ["hopper_fill"]),
        ("MIXER",   24,  ["mixer_rpm", "mixer_vibration"]),
        ("EXTRUDER", 40, ["extruder_pressure"]),
        ("COOKER",  56,  ["cook_temp_1", "cook_temp_2", "steam_pressure"]),
        ("COOLER",  72,  ["cooler_temp"]),
        ("PACKAGER", 88, ["belt_speed", "sausage_count"]),
    ]

    BOX_W, BOX_H, Y_MID = 14, 22, 30

    fig = go.Figure()
    fig.update_layout(
        xaxis=dict(range=[0, 100], visible=False),
        yaxis=dict(range=[0, 62], visible=False),
        height=320,
        margin=dict(l=0, r=0, t=0, b=0),
        plot_bgcolor=BG,
        paper_bgcolor=BG,
        font=dict(color="#cccccc", family="monospace"),
    )

    for i, (label, xc, sensors) in enumerate(stages):
        x0, x1 = xc - BOX_W / 2, xc + BOX_W / 2
        y0, y1 = Y_MID - BOX_H / 2, Y_MID + BOX_H / 2

        statuses = [sensor_status(s, vals[s]) for s in sensors if s in vals]
        box_status = ("alarm" if "alarm" in statuses
                       else "warning" if "warning" in statuses
                       else "ok" if statuses else "idle")
        colour = STAGE_COLOURS[box_status]
        fill = ("rgba(0,204,102,0.12)" if box_status == "ok" else
                "rgba(255,170,0,0.12)" if box_status == "warning" else
                "rgba(255,68,68,0.12)" if box_status == "alarm" else
                "rgba(80,80,100,0.15)")

        fig.add_shape(type="rect", x0=x0, y0=y0, x1=x1, y1=y1,
                      line=dict(color=colour, width=2), fillcolor=fill)
        fig.add_annotation(x=xc, y=y1 + 1.5, text=f"<b>{label}</b>",
                           showarrow=False, font=dict(size=9, color=colour),
                           xanchor="center", yanchor="bottom")

        n = len(sensors)
        spacing = min(6.0, BOX_H / (n + 1))
        for j, sid in enumerate(sensors):
            y_pos = Y_MID + (j - (n - 1) / 2) * spacing
            val = vals.get(sid)
            unit = DEVICES[sid]["unit"]
            val_str = f"{val:.1f} {unit}" if val is not None else "—"
            c = STAGE_COLOURS[sensor_status(sid, val) if val is not None else "idle"]
            fig.add_annotation(x=xc, y=y_pos, text=f"<b>{val_str}</b>",
                               showarrow=False, font=dict(size=10, color=c),
                               xanchor="center", yanchor="middle")

        if i < len(stages) - 1:
            next_xc = stages[i + 1][1]
            fig.add_annotation(x=(x1 + next_xc - BOX_W / 2) / 2, y=Y_MID, text="▶",
                               showarrow=False, font=dict(size=14, color="#444466"),
                               xanchor="center", yanchor="middle")

    return fig


# ── Compliance summary ────────────────────────────────────────────────────────

def compliance_summary(violations: pd.DataFrame) -> tuple[int, str]:
    n = len(violations)
    if n == 0:
        return 0, "OK"
    if n < COMPLIANCE_CRITICAL_THRESHOLD:
        return n, "WARNING"
    return n, "CRITICAL"


# ── Vibration alert ───────────────────────────────────────────────────────────

def vibration_alert(vib_df: pd.DataFrame) -> tuple[str, float | None]:
    if vib_df.empty:
        return "NO DATA", None
    short = vib_df.iloc[-1]["mean_vibration"]
    if len(vib_df) >= 10:
        baseline = vib_df["mean_vibration"].iloc[-10:].mean()
        if short > baseline * VIBRATION_ALERT_RATIO:
            return "ALERT", short
    return "OK", short


# ── Layout ────────────────────────────────────────────────────────────────────

st.markdown(
    "<h2 style='margin-bottom:0'>🌿 Verdant Veggie Sausage Co. — Production Monitor</h2>",
    unsafe_allow_html=True,
)

violations = compliance_violations(m_buf)
viol_count, compliance_status = compliance_summary(violations)
status_colour = {"OK": "green", "WARNING": "orange", "CRITICAL": "red"}.get(compliance_status, "grey")
lag_active = LAG_FLAG.exists()

header_l, header_r = st.columns([3, 1])
with header_l:
    st.markdown(f"Compliance: :{status_colour}[**{compliance_status}**] &nbsp;·&nbsp; {viol_count} violation(s) today")
with header_r:
    lag = data_lag_seconds(t_buf)
    if lag is not None:
        st.markdown(f"Data lag: **{lag:.1f}s**")

# SCADA diagram
st.subheader("Production Line")
readings = latest_sensor_values(t_buf)
if not readings.empty:
    st.plotly_chart(build_scada(readings), width='stretch')
else:
    st.info("Waiting for telemetry — is the simulator and bridge running?")

st.divider()

# Metrics cards
st.subheader("Calculated Metrics")
m1, m2, m3, m4 = st.columns(4)

with m1:
    st.markdown("**Mean Cook Temp** *(2-min sliding window)*")
    tdf = temp_window_metrics(m_buf)
    if not tdf.empty:
        latest_temp = tdf.iloc[-1]["mean_temp"]
        st.metric("Latest window", f"{latest_temp:.1f} °C")
        chart = tdf.copy()
        chart.index = pd.to_datetime(chart["window_start"]).dt.strftime("%H:%M:%S")
        st.line_chart(chart["mean_temp"].rename("°C"), height=220)
        if latest_temp < FOOD_SAFETY_MIN_TEMP:
            st.error(f"Below safety minimum ({FOOD_SAFETY_MIN_TEMP} °C)")
    else:
        st.caption("No data yet")

with m2:
    st.markdown("**Hourly Production**")
    pdf = production_hourly(m_buf)
    if not pdf.empty:
        st.metric("This hour", f"{int(pdf.iloc[-1]['total_sausages']):,} sausages")
        chart = pdf.tail(8).copy()
        chart.index = pd.to_datetime(chart["hour"]).dt.strftime("%H:%M:%S")
        st.bar_chart(chart["total_sausages"].rename("sausages"), height=220)
    else:
        st.caption("No data yet")

with m3:
    st.markdown("**Food Safety Compliance**")
    st.metric("Violations today", viol_count,
              delta=None if viol_count == 0 else f"+{viol_count}",
              delta_color="inverse")
    if not violations.empty:
        display = violations[["window_start", "mean_temp"]].copy()
        display.columns = ["Window", "Mean °C"]
        display["Mean °C"] = display["Mean °C"].round(1)
        st.dataframe(display.head(5), width='stretch', hide_index=True)
    else:
        st.success("No violations recorded today")

with m4:
    st.markdown("**Mixer Vibration Health**")
    vdf = vibration_window_metrics(m_buf)
    alert_status, current_vib = vibration_alert(vdf)
    if alert_status == "ALERT":
        st.metric("10-min mean", f"{current_vib:.3f} g")
        st.error("Vibration above baseline × 1.5 — check mixer")
    elif alert_status == "OK" and current_vib is not None:
        st.metric("10-min mean", f"{current_vib:.3f} g")
        st.success("Normal")
    else:
        st.caption("No data yet")
    if not vdf.empty:
        chart = vdf.tail(20).copy()
        chart.index = pd.to_datetime(chart["window_start"]).dt.strftime("%H:%M:%S")
        st.line_chart(chart["mean_vibration"].rename("g"), height=220)

st.divider()

# Stream tails
st.subheader("Stream Tails")

if not st.session_state.get("readers_ready"):
    st.warning("Pulsar stream readers not connected — is Pulsar running?")
else:
    tail_l, tail_r = st.columns(2)

    with tail_l:
        st.markdown("**Telemetry stream**")
        if t_buf:
            tdf = pd.DataFrame(t_buf)
            device_options = sorted(tdf["device_id"].unique().tolist())
            selected_devices = st.multiselect("Filter devices", device_options,
                                              key="telem_filter", placeholder="All devices")
            display_tdf = tdf[tdf["device_id"].isin(selected_devices)] if selected_devices else tdf
            st.dataframe(display_tdf[["timestamp", "device_id", "value"]].head(200),
                         height=400, width="stretch", hide_index=True)
        else:
            st.caption("Waiting for messages…")

    with tail_r:
        st.markdown("**Metrics stream**")
        if m_buf:
            mdf = pd.DataFrame(m_buf)
            metric_options = sorted(mdf["metric"].unique().tolist())
            selected_metrics = st.multiselect("Filter metrics", metric_options,
                                              key="metrics_filter", placeholder="All metrics")
            display_mdf = mdf[mdf["metric"].isin(selected_metrics)] if selected_metrics else mdf
            st.dataframe(display_mdf.head(200), height=400, width="stretch", hide_index=True)
        else:
            st.caption("Waiting for messages…")

st.divider()

# Eventual consistency demo
st.subheader("Eventual Consistency Demo")
ctrl_l, ctrl_r = st.columns([2, 2])

with ctrl_l:
    if lag_active:
        st.warning(
            "**Sensor lag is ON.** "
            "`sausage_count` is buffering and will flush as a late batch (60–90 s). "
            "`cook_temp_1` is sending readings with a 30–45 s timestamp lag. "
            "Watch the production counter jump and the temperature average self-correct "
            "when delayed data arrives."
        )
        if st.button("Stop sensor lag"):
            LAG_FLAG.unlink(missing_ok=True)
            st.rerun()
    else:
        st.info(
            "Press the button to simulate sensor lag. "
            "The production counter will briefly under-count, "
            "then jump when the buffered batch arrives. "
            "The cook temperature average will be initially calculated on incomplete data, "
            "then silently corrected by Spark when the delayed readings land."
        )
        if st.button("⚡ Simulate sensor lag"):
            LAG_FLAG.parent.mkdir(parents=True, exist_ok=True)
            LAG_FLAG.touch()
            st.rerun()

with ctrl_r:
    st.markdown("**How eventual consistency works here**")
    st.markdown(
        "Spark's watermark allows events up to **2 minutes late** to be included in "
        "their original windows. When a late event arrives:\n"
        "1. Spark re-evaluates the affected window\n"
        "2. An updated metric message is published to the Pulsar metrics topic\n"
        "3. The UI reader picks up the correction on the next refresh\n\n"
        "No manual intervention — the system self-heals."
    )

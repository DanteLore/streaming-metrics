import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import sqlite3
from datetime import datetime, timezone

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from shared.schema import (
    COMPLIANCE_CRITICAL_THRESHOLD,
    DB_PATH,
    DEVICES,
    FOOD_SAFETY_MIN_TEMP,
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
BOX_BG = "rgba(255,255,255,0.05)"


# ── data helpers ─────────────────────────────────────────────────────────────

def _query(sql: str, params: tuple = ()) -> pd.DataFrame:
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query(sql, conn, params=params)
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()


def latest_readings() -> pd.DataFrame:
    return _query("SELECT * FROM latest_readings")


def temp_metrics() -> pd.DataFrame:
    return _query(
        "SELECT window_start, window_end, "
        "CAST(temp_sum AS REAL) / temp_count AS mean_temp, "
        "temp_count, computed_at "
        "FROM temp_metrics ORDER BY window_start DESC LIMIT 40"
    )


def production_hourly() -> pd.DataFrame:
    return _query(
        "SELECT hour, total_sausages, computed_at "
        "FROM production_hourly ORDER BY hour DESC LIMIT 24"
    )


def compliance_violations() -> pd.DataFrame:
    return _query(
        "SELECT window_start, window_end, mean_temp, computed_at "
        "FROM compliance_violations ORDER BY window_start DESC LIMIT 50"
    )


def vibration_metrics() -> pd.DataFrame:
    return _query(
        "SELECT window_start, window_end, mean_vibration, computed_at "
        "FROM vibration_metrics ORDER BY window_start DESC LIMIT 60"
    )


# ── sensor status ─────────────────────────────────────────────────────────────

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

    # (label, x_centre, [sensor_ids])
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
        fill = f"rgba(0,204,102,0.12)" if box_status == "ok" else \
               f"rgba(255,170,0,0.12)" if box_status == "warning" else \
               f"rgba(255,68,68,0.12)" if box_status == "alarm" else \
               "rgba(80,80,100,0.15)"

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
            status = sensor_status(sid, val) if val is not None else "idle"
            c = STAGE_COLOURS[status]
            fig.add_annotation(
                x=xc, y=y_pos,
                text=f"<b>{val_str}</b>",
                showarrow=False,
                font=dict(size=10, color=c),
                xanchor="center", yanchor="middle",
            )

        # Arrow to next stage
        if i < len(stages) - 1:
            next_xc = stages[i + 1][1]
            arrow_x = (x1 + next_xc - BOX_W / 2) / 2
            fig.add_annotation(x=arrow_x, y=Y_MID, text="▶",
                               showarrow=False, font=dict(size=14, color="#444466"),
                               xanchor="center", yanchor="middle")

    return fig


# ── compliance helpers ────────────────────────────────────────────────────────

def compliance_summary(violations: pd.DataFrame) -> tuple[int, str]:
    n = len(violations)
    if n == 0:
        return 0, "OK"
    if n < COMPLIANCE_CRITICAL_THRESHOLD:
        return n, "WARNING"
    return n, "CRITICAL"


# ── vibration alert ───────────────────────────────────────────────────────────

def vibration_alert(vib_df: pd.DataFrame) -> tuple[str, float | None]:
    if vib_df.empty:
        return "NO DATA", None
    short = vib_df.iloc[0]["mean_vibration"]
    if len(vib_df) >= 10:
        baseline = vib_df.head(10)["mean_vibration"].mean()
        if short > baseline * VIBRATION_ALERT_RATIO:
            return "ALERT", short
    return "OK", short


# ── layout ────────────────────────────────────────────────────────────────────

st.markdown(
    "<h2 style='margin-bottom:0'>🌿 Verdant Veggie Sausage Co. — Production Monitor</h2>",
    unsafe_allow_html=True,
)

readings = latest_readings()
violations = compliance_violations()
viol_count, compliance_status = compliance_summary(violations)

status_colour = {"OK": "green", "WARNING": "orange", "CRITICAL": "red"}.get(compliance_status, "grey")
lag_active = LAG_FLAG.exists()

header_l, header_r = st.columns([3, 1])
with header_l:
    st.markdown(
        f"Compliance: :{status_colour}[**{compliance_status}**] &nbsp;·&nbsp; "
        f"{viol_count} violation(s) today",
        unsafe_allow_html=False,
    )
with header_r:
    if not readings.empty:
        last_rx = pd.to_datetime(readings["received_at"]).max()
        if last_rx.tzinfo is None:
            last_rx = last_rx.tz_localize("UTC")
        lag_secs = (datetime.now(tz=timezone.utc) - last_rx).total_seconds()
        st.markdown(f"Data lag: **{lag_secs:.1f}s**")

# SCADA diagram
st.subheader("Production Line")
if not readings.empty:
    st.plotly_chart(build_scada(readings), width='stretch')
else:
    st.info("Waiting for telemetry — is the simulator and bridge running?")

st.divider()

# Metrics row
st.subheader("Calculated Metrics")
m1, m2, m3, m4 = st.columns(4)

with m1:
    st.markdown("**Mean Cook Temp** *(2-min sliding window)*")
    tdf = temp_metrics()
    if not tdf.empty:
        latest_temp = tdf.iloc[0]["mean_temp"]
        delta_colour = "normal" if latest_temp >= FOOD_SAFETY_MIN_TEMP else "inverse"
        st.metric("Latest window", f"{latest_temp:.1f} °C")
        chart_data = (
            tdf.set_index("window_start")["mean_temp"]
            .sort_index()
            .rename("°C")
        )
        st.line_chart(chart_data, height=150)
        # Show a safety threshold reference line note
        if latest_temp < FOOD_SAFETY_MIN_TEMP:
            st.error(f"Below safety minimum ({FOOD_SAFETY_MIN_TEMP} °C)")
    else:
        st.caption("No data yet")

with m2:
    st.markdown("**Hourly Production**")
    pdf = production_hourly()
    if not pdf.empty:
        this_hour = pdf.iloc[0]
        st.metric("This hour", f"{int(this_hour['total_sausages']):,} sausages")
        chart_data = (
            pdf.set_index("hour")["total_sausages"]
            .sort_index()
            .tail(8)
            .rename("sausages")
        )
        st.bar_chart(chart_data, height=150)
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
    vdf = vibration_metrics()
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
        chart_data = (
            vdf.set_index("window_start")["mean_vibration"]
            .sort_index()
            .tail(20)
            .rename("g")
        )
        st.line_chart(chart_data, height=150)

st.divider()

# Eventual consistency demo controls
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
        "2. The updated result replaces the previous value in SQLite\n"
        "3. The UI picks up the correction on the next refresh\n\n"
        "No manual intervention — the system self-heals."
    )

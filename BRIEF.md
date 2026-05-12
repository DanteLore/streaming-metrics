# Streaming Metrics - Technology Proof of Concept

This project demonstrates, using simple code and minimal local tooling, a system that calculates arbitrary metrics from an incoming telemetry stream. The demo scenario is a **vegetarian sausage production line** — chosen because it has a natural flow of stages, a plausible set of sensors, clear food-safety constraints, and is more interesting than generic IoT data.

---

## Goals

* Demonstrate that Apache Spark can generate real-time metrics from a live telemetry stream.
* Demonstrate **eventual consistency**: show metrics updating automatically when late-arriving or out-of-order events land, without any manual intervention.
* Produce example Spark jobs/functions showing how metrics of varying complexity can be structured.

---

## Technology Stack

| Concern | Choice | Notes |
|---|---|---|
| Streaming backbone | Apache Pulsar 3.3 | Hard constraint. Run via Docker Compose, vanilla install. |
| Stream processing | PySpark Structured Streaming 3.3.x | Micro-batch model. Pinned to 3.3.x for compatibility with the Pulsar-Spark connector. |
| Pulsar-Spark connector | StreamNative `pulsar-spark-connector_2.12:3.3.0.1` | Allows Spark to read directly from a Pulsar topic as a streaming source. Downloaded automatically via `spark.jars.packages` on first run. |
| Telemetry simulator | Python script | Publishes to Pulsar. Simulates realistic + deliberately late/OOO events. |
| UI | Streamlit | All-Python. Auto-refreshes to show live updates. Reads directly from Pulsar using the Reader API. |

No paid tools or services.

---

## Architecture

All data flows through Pulsar. There is no file-based intermediate layer.

```
[simulator/simulate.py]
        │
        │  publishes telemetry events (~1Hz)
        ▼
[Pulsar: telemetry topic]
        │                         │
        │ readStream               │ Reader API (non-destructive)
        ▼ (Pulsar-Spark connector) │
[metrics/jobs.py]                  │
  PySpark Structured Streaming      │
        │                          │
        │ foreachBatch             │
        ▼                          │
[Pulsar: metrics topic]            │
        │                          │
        │ Reader API               │
        ▼                          ▼
              [ui/app.py]
              Streamlit dashboard
              (reads both topics directly)
```

Spark reads the telemetry topic as a native streaming source, computes windowed metrics, and publishes results back to the metrics topic. The UI reads from both topics using Pulsar Readers — non-destructive reads that don't require a subscription or affect message delivery. There is no bridge process, no file landing zone, and no intermediate database.

---

## The Production Line

The simulated machine processes vegetarian sausages through six stages. Each stage has one or more sensors publishing telemetry.

```
[HOPPER] → [MIXER] → [EXTRUDER] → [STEAM COOKER] → [COOLING TUNNEL] → [PACKAGER]
```

### Sensors (telemetry sources)

Each reading has the same envelope:

```json
{
  "device_id": "cook_temp_1",
  "value": 78.4,
  "timestamp": "2024-01-15T10:23:45.123Z"
}
```

| Device ID | Stage | Unit | Normal range | Description |
|---|---|---|---|---|
| `hopper_fill` | Hopper | % | 20–95 | Ingredient hopper fill level |
| `mixer_rpm` | Mixer | RPM | 60–120 | Blending motor speed |
| `mixer_vibration` | Mixer | g | 0.1–1.5 | Vibration — spikes indicate imbalance |
| `extruder_pressure` | Extruder | bar | 3–8 | Forming head pressure |
| `cook_temp_1` | Steam cooker (entry) | °C | 72–95 | Temperature at cooker inlet |
| `cook_temp_2` | Steam cooker (exit) | °C | 72–95 | Temperature at cooker outlet |
| `steam_pressure` | Steam cooker | bar | 1.5–3.0 | Steam supply pressure |
| `belt_speed` | Conveyor | m/min | 0.5–2.0 | Conveyor belt speed |
| `cooler_temp` | Cooling tunnel | °C | 2–8 | Cold tunnel temperature |
| `sausage_count` | Packager | count | — | Sausages passing the counter per reading |

The simulator publishes all sensors at roughly 1Hz, with occasional deliberate anomalies (see Eventual Consistency section).

---

## Metrics

Metrics are calculated by Spark Structured Streaming and published back to a separate Pulsar topic that the UI consumes.

### 1. Simple sliding-window averages

Mean cook temperature (both zones) over a **1-minute sliding window, updated every 30 seconds**. Stored as SUM + COUNT so windows can be merged correctly when late data arrives.

### 2. Production counters

* **Sausages per hour** — running total from `sausage_count` events, reset on the hour.

This is a good subject for the eventual consistency demo because a batch of late `sausage_count` events causes the counter to visibly jump and self-correct.

### 3. Complex compliance metric — Cook Temperature Safety

Vegetarian sausages must reach a minimum core temperature. The rule: **cook temperature must not fall below 72°C**. The compliance metric tracks:

* Number of **violation windows** today: a violation window is any 1-minute tumbling window in which the mean cook temperature (average of `cook_temp_1` and `cook_temp_2`) is below 72°C.
* A **compliance status** field: `OK` / `WARNING` (1 violation) / `CRITICAL` (2+ violations).
* A daily violation log with timestamps.

This demonstrates that Spark can evaluate stateful, threshold-based rules over time — not just aggregations. The metric updates live as new temperature readings arrive, and self-corrects if readings were delayed and initially caused a false violation (eventual consistency).

### 4. Equipment health metric — Mixer vibration trend

The mean mixer vibration over a **2-minute sliding window** compared against the rolling baseline. If the short-window mean exceeds 1.5× the baseline, raise a `MIXER_ALERT`. This shows a relative/derived metric rather than an absolute threshold.

---

## Eventual Consistency Demo

This is a key aim. The simulator will deliberately produce two classes of anomaly to tell the story:

### Scenario A — Late batch (production counter)

The `sausage_count` sensor at the packager occasionally buffers readings locally (simulating a flaky network) and then flushes a batch 60–90 seconds late. The UI will show the hourly counter sitting lower than reality, then visibly jumping upward when the late batch lands and Spark reprocesses the affected window.

### Scenario B — Out-of-order temperature events

The `cook_temp_1` sensor occasionally sends readings with timestamps that are 30–45 seconds behind wall-clock time (simulating a slow edge device). This causes the sliding-window temperature average to be initially calculated on incomplete data, then silently corrected when the delayed readings arrive within Spark's configured watermark window (2 minutes). The UI shows the metric value update without any user action.

Both scenarios are triggered by a button in the Streamlit UI which creates a flag file (`data/flags/simulate_lag`). The simulator polls for the flag and adjusts its behaviour accordingly.

---

## User Interface

Built in Streamlit, auto-refreshing every 2 seconds. Reads both Pulsar topics directly using the Pulsar Reader API, maintaining a rolling in-memory buffer in `st.session_state`.

### SCADA View

A schematic diagram of the production line showing the six stages in sequence. Live sensor values update in place. Colour coding: green = normal range, amber = approaching limit, red = out of range.

### Metrics Dashboard

Card layout showing all four calculated metrics with charts. Highlights food-safety violations and vibration alerts.

### Stream Tails

Live tail of both the telemetry and metrics topics, filterable by device/metric type. Shows the raw stream order, including the eventual-consistency corrections arriving as updated metric messages.

### Eventual Consistency Demo Panel

Toggle button to activate/deactivate sensor lag simulation, with an explanation of how Spark's watermark model handles the corrections automatically.

---

## Project Structure

```
streaming-metrics/
├── docker-compose.yml        # Pulsar standalone
├── requirements.txt
├── simulator/
│   └── simulate.py           # Telemetry generator and Pulsar publisher
├── metrics/
│   └── jobs.py               # PySpark Structured Streaming jobs
├── ui/
│   └── app.py                # Streamlit dashboard
├── shared/
│   ├── schema.py             # Shared constants (topics, device definitions, thresholds)
│   └── pulsar_streams.py     # Pulsar client wrapper (make_client, make_reader, drain, etc.)
└── docs/
    └── spark-vs-flink.md     # Spark vs Flink trade-off comparison
```

---

## Out of Scope

* High-throughput or performance testing — the demo is illustrative, not a load test.
* Authentication, TLS, or multi-tenant Pulsar configuration.
* Persistent storage — all state lives in Pulsar topics and Spark checkpoints.
* Deployment outside a local development machine.

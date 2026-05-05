# Streaming Metrics — Technology Proof of Concept

> **This is a feasibility study, not production code.** The goal is to answer the question: *can Apache Spark compute real-time metrics directly from a Pulsar stream, demonstrate eventual consistency, and surface the results live in a UI — using only open-source tooling?* The answer is yes. The code is deliberately simple and illustrative.

---

## What this demonstrates

1. **Stream-native metric calculation** — PySpark Structured Streaming reads directly from a Pulsar topic, computes windowed metrics, and publishes results back to a second Pulsar topic. No intermediate database, no file landing zone.

2. **Eventual consistency** — Spark's watermark model automatically reprocesses windows when late or out-of-order events arrive. The UI self-corrects without any manual intervention. Two scenarios are included: a simulated network-buffered late batch, and a slow sensor sending stale timestamps.

3. **Metrics of varying complexity** — from simple sliding-window averages, through hourly production counters, to threshold-based food-safety compliance and relative equipment-health alerts.

4. **Live SCADA-style dashboard** — Streamlit reads both Pulsar topics directly using the Reader API and displays a production line schematic, metric charts, and raw stream tails — all auto-refreshing every 2 seconds.

---

## Demo scenario

The simulated system is a **vegetarian sausage production line**, chosen for its natural stage flow, plausible sensor set, and clear food-safety constraints.

```
[HOPPER] → [MIXER] → [EXTRUDER] → [STEAM COOKER] → [COOLING TUNNEL] → [PACKAGER]
```

Ten sensors publish telemetry at ~1Hz. Spark computes four metrics from the stream:

| Metric | Type | Window |
|---|---|---|
| Mean cook temperature | Sliding average | 2 min, 30-sec slide |
| Sausages per hour | Running counter | 1-hour tumbling |
| Food-safety compliance | Threshold violation log | 2-min tumbling |
| Mixer vibration health | Relative trend alert | 10-min sliding vs baseline |

---

## Architecture

All data flows through Pulsar.

```
[simulator/simulate.py]
        │
        │  publishes telemetry (~1Hz)
        ▼
[Pulsar: telemetry topic]
        │                         │
        │ Pulsar-Spark connector  │ Reader API
        ▼                         │
[metrics/jobs.py]                 │
  PySpark Structured Streaming    │
        │                         │
        │ foreachBatch            │
        ▼                         │
[Pulsar: metrics topic]           │
        │                         │
        │ Reader API              │
        ▼                         ▼
            [ui/app.py]
            Streamlit dashboard
```

---

## Architecture in detail

### Data flow

The simulator publishes JSON telemetry messages to the Pulsar `telemetry` topic at ~1Hz. Each message carries a `device_id`, a `value`, and an event `timestamp`.

Spark reads the topic as a native streaming source via the StreamNative connector. The connector handles offset tracking — Spark knows exactly which messages it has processed, and checkpoints that state to disk so it can resume after a restart without reprocessing or missing events.

Spark applies a **2-minute watermark** to the event timestamp. This tells Spark: "accept events up to 2 minutes late; after that, close the window." When a late event arrives within the watermark, Spark reprocesses the affected window and emits an updated result. Each result is published to the Pulsar `metrics` topic as a new message — the UI picks it up on the next refresh, and the displayed value silently corrects itself. This is the eventual consistency mechanic.

The Streamlit UI maintains a rolling in-memory buffer of the last 500 messages from each topic, held in `st.session_state` across reruns. It uses Pulsar **Readers** (not Consumers) — Readers are non-destructive and non-subscribing, so the UI can read from any position in the topic without affecting Spark's offset tracking or message retention.

### Windowing strategy

| Metric | Window type | Size | Slide |
|---|---|---|---|
| Mean cook temp | Sliding | 2 min | 30 sec |
| Sausages per hour | Tumbling | 1 hour | — |
| Compliance check | Tumbling | 2 min | — |
| Vibration health | Sliding | 10 min | 1 min |

Sliding windows overlap — a single event contributes to multiple windows. This means more Spark state but smoother metric output. Tumbling windows are non-overlapping and cheaper; used where a hard boundary makes domain sense (hourly counts, compliance periods).

The temperature metric publishes `temp_sum` and `temp_count` separately rather than a pre-computed mean. This allows a corrected window (with additional late events) to produce a correct updated mean without needing to store the raw events.

### Pulsar topics

| Topic | Publisher | Consumers |
|---|---|---|
| `persistent://public/default/telemetry` | `simulator/simulate.py` | Spark (via connector), Streamlit (Reader) |
| `persistent://public/default/metrics` | `metrics/jobs.py` (foreachBatch) | Streamlit (Reader) |

---

## Key components

### `simulator/simulate.py`
Publishes all ten sensor readings at roughly 1Hz using the Pulsar Python client. In normal operation, values are randomised within each sensor's normal range with occasional deliberate excursions to trigger warnings. When the lag flag is set (`data/flags/simulate_lag`), two anomalies activate simultaneously: `sausage_count` buffers locally and flushes a late batch after 60–90 seconds; `cook_temp_1` sends readings with timestamps backdated 30–45 seconds.

### `metrics/jobs.py`
Four PySpark Structured Streaming queries running in parallel, each with its own checkpoint location. Reads from the telemetry topic, applies watermarking, runs windowed aggregations, and publishes results back to the metrics topic via `foreachBatch`. Each batch creates a fresh Pulsar producer, sends the records, flushes, and closes — keeping the Spark executor side stateless with respect to Pulsar connections.

### `shared/pulsar_streams.py`
Thin wrapper around the Pulsar Python client — `make_client`, `make_producer`, `publish_batch`, `make_reader`, `drain`. No domain logic, no UI dependencies. Used by both the Spark job (producer side) and the Streamlit app (reader side).

### `shared/schema.py`
All shared constants: topic names, URLs, device definitions (normal operating ranges and units), food-safety threshold, vibration alert ratio, compliance critical threshold.

### `ui/app.py`
Streamlit dashboard. Manages two Pulsar Readers in `st.session_state` (one per topic), drains new messages into rolling buffers on each 2-second autorefresh, and renders: a Plotly SCADA diagram, four metric cards with charts, filterable stream tail tables, and the eventual consistency demo controls. All buffer query logic (deduplication, window filtering, metric calculation) lives directly in this file as it is not shared.

---

## Stack

| Concern | Choice |
|---|---|
| Streaming backbone | Apache Pulsar 3.3 (Docker) |
| Stream processing | PySpark Structured Streaming 3.5.x |
| Pulsar-Spark connector | StreamNative `pulsar-spark-connector_2.12:3.4.0.4` |
| UI | Streamlit |
| Language | Python throughout |

No paid tools or services. Runs entirely on a local machine.

---

## Running it

**Prerequisites:** Docker Desktop, Python 3.11+, Java 17, tmux.

```bash
brew install tmux openjdk@17
```

```bash
# Add Java to ~/.zshrc
export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
export PATH="$JAVA_HOME/bin:$PATH"

# Install Python dependencies
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Start everything
./start.sh
```

`start.sh` opens a tmux session with four panes — Pulsar, Spark, simulator, and the Streamlit UI. Each process waits for Pulsar to be healthy before starting. On first run, Spark downloads the connector JAR from Maven Central (~50MB); subsequent starts use the cache.

The dashboard is at **http://localhost:8501**.

To stop everything: `tmux kill-session -t streaming-metrics`

---

## Eventual consistency demo

In the Streamlit UI, press **⚡ Simulate sensor lag** to activate two simultaneous anomalies:

- **`sausage_count`** buffers for 60–90 seconds then flushes a late batch — the production counter visibly jumps when the data lands.
- **`cook_temp_1`** sends readings with timestamps 30–45 seconds in the past — the temperature average is initially calculated on incomplete data, then silently corrected when the delayed events arrive within Spark's 2-minute watermark window.

Press **Stop sensor lag** to return to normal operation.

---

## What this isn't

- A load or performance test
- Production-ready code (no auth, no TLS, no error recovery, single-node everything)
- A Flink comparison (see [docs/spark-vs-flink.md](docs/spark-vs-flink.md) for trade-offs)

---

## Questions for a production build

The following are open questions a team would need to answer before building this at scale (~7 billion rows per day, multi-tenant, cloud-deployed). These are starting points for investigation, not complete answers.

---

### Throughput and partitioning

**Can Pulsar handle 7B rows/day (~80k msg/sec)?**
Almost certainly yes at the Pulsar layer — Pulsar is designed for high throughput and horizontal scaling. The constraint is likely the broker and BookKeeper tier sizing, not the protocol. Worth investigating: topic partitioning (each partition can be consumed independently), tiered storage offloading to S3/GCS for older segments, and whether the message schema (JSON here) should be replaced with a binary format (Avro, Protobuf) to reduce payload size and serialisation overhead.

**How do we partition the telemetry topic?**
A single partition is a single sequential log — fine for a demo, a bottleneck at scale. Partitioning by device ID or site ID would allow Spark and downstream consumers to parallelise. Investigate how partition count interacts with Spark's task parallelism and whether device-keyed partitioning causes hot partitions for high-frequency sensors.

---

### Spark at scale

**Where does Spark run in AWS?**
Three main options to investigate:
- **Amazon EMR** — managed Spark, straightforward, good integration with S3 for checkpoints. Easiest path.
- **EMR Serverless** — no cluster to manage, scales to zero, pay-per-use. Worth evaluating for bursty or intermittent workloads.
- **Kubernetes (EKS) with Spark Operator** — most control, best for teams already running EKS. Higher operational overhead.

**How do we size the Spark cluster?**
This demo runs on a single local JVM. At 80k msg/sec, shuffle partitions, executor memory, and parallelism all need tuning. Investigate: how many executors are needed for the watermark state store at peak throughput, whether RocksDB state backend is needed over the default in-memory store, and how checkpoint frequency affects latency vs recovery time.

**How do we handle Spark job failure and recovery?**
Structured Streaming with checkpoints provides exactly-once processing guarantees — on restart it resumes from the last committed offset. Investigate: checkpoint storage on S3 (latency implications), whether `foreachBatch` idempotency is guaranteed if a batch partially completes, and how to handle schema evolution in the checkpoint state.

---

### Metric complexity and state

**What happens when metric definitions change?**
Changing a window size or adding a new aggregation on a running stream requires either a clean restart (dropping checkpoint state) or a careful migration. Investigate: blue/green Spark job deployments, state schema migration strategies, and whether separating stateless and stateful jobs makes versioning easier.

**How do we support many different metric definitions without redeploying Spark?**
Currently metric logic is hardcoded in `jobs.py`. At scale, different teams may want different metrics over the same stream. Investigate: a metric configuration store (database-driven window definitions), dynamic Spark job generation, or a rules engine sitting between the raw stream and the metrics topic.

---

### Data quality and late data

**What is the right watermark for production data?**
The 2-minute watermark here is illustrative. In production, the acceptable lateness depends on the source (edge device, cloud sensor, batch export) and the metric's business use (real-time alerting vs daily reporting). Investigate: per-source watermark policies, whether some metrics need separate streams with different watermarks, and how to monitor actual event lag vs the configured watermark.

**How do we detect and handle corrupt or missing telemetry?**
The simulator always produces clean data. In production, sensors drop out, send nulls, or repeat stale values. Investigate: dead-sensor detection (no events for N seconds), outlier filtering before windowing, and how Spark handles null values in aggregations.

---

### Infrastructure and operations

**How does Pulsar scale horizontally in AWS?**
Pulsar separates compute (brokers) from storage (BookKeeper). Brokers are stateless and can be scaled independently. Investigate: running Pulsar on EKS vs using a managed service (StreamNative Cloud, Datastax Astra Streaming), cross-AZ BookKeeper quorum sizing, and whether Pulsar Functions are worth using for lightweight per-message transforms.

**How do we monitor the pipeline health?**
This demo has no observability beyond Streamlit's data lag indicator. Investigate: Pulsar's built-in metrics (Prometheus endpoint), Spark Structured Streaming metrics (query progress, input rate, processing rate), and alerting on watermark lag as a proxy for pipeline health.

**How do we manage schema evolution?**
JSON with no schema registry works for a demo but becomes a liability when producers and consumers evolve independently. Investigate: Apache Avro with Pulsar's built-in schema registry, schema compatibility modes (backward, forward), and how the Spark connector handles schema changes in flight.

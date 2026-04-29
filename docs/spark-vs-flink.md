# Spark vs Flink for stream processing

| Feature | Apache Spark | Apache Flink |
|---|---|---|
| **Processing model** | Micro-batch (default). Continuous processing exists but is experimental and limited | True event-at-a-time streaming. Results emitted as events arrive |
| **Latency** | Seconds (bounded by trigger interval — 10 s in this project) | Sub-second |
| **Pulsar integration** | Third-party StreamNative connector; version lags Spark releases; needs Spark downgrade to 3.3 for reliable support | Official Apache `flink-connector-pulsar`; first-class, actively maintained |
| **Kafka integration** | First-class built-in connector; battle-tested | First-class built-in connector; battle-tested |
| **Watermarks / late data** | Supported. Single global watermark per stream | Supported. More expressive — per-key watermarks, custom triggers, richer window types |
| **Python support** | PySpark — mature, well-documented, large community, first-class API | PyFlink — functional but thin docs, sparse community, DataStream API incomplete in Python; Table API / SQL better supported |
| **Primary language** | Python (PySpark) or Scala | Java (primary); Scala (deprioritised since 1.15); Python (limited) |
| **Batch + streaming** | Unified API — same code runs on batch or streaming | Streaming-first; batch support added later; less unified feel |
| **State management** | Simple; RocksDB-backed state for large state use cases | Sophisticated; RocksDB with incremental checkpoints; more control over state TTL and eviction |
| **Windowing** | Tumbling, sliding, session windows | Same, plus global windows, custom triggers, interval joins |
| **Deployment complexity** | Driver + Executors; local mode straightforward | JobManager + TaskManagers; local mode straightforward |
| **Ecosystem / tooling** | Very large; Databricks, Delta Lake, MLlib, Spark SQL all integrate naturally | Smaller but growing; strong in telco and finance streaming use cases |
| **Community / Stack Overflow** | Very large | Moderate |

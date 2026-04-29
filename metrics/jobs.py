"""PySpark Structured Streaming jobs.

Reads NDJSON files from the landing zone (written by the bridge from the Pulsar
telemetry topic), calculates four metrics with proper watermarking, then publishes
each result back to the Pulsar metrics topic via foreachBatch.

A separate metrics consumer reads from the metrics topic and writes to SQLite for
the UI. Pulsar is the sole integration point for both input and output.

The watermark is set to 2 minutes so that late-arriving events (up to ~90 seconds
late in the lag-simulation scenario) are still included in their correct windows.
When Spark reprocesses a window after late data arrives, an updated message is
published to the metrics topic — this is the eventual-consistency correction.
"""
import json
from datetime import datetime, timezone
from pathlib import Path

import pulsar
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, sum, to_timestamp, window
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from shared.schema import (
    CHECKPOINT_DIR,
    LANDING_DIR,
    METRICS_TOPIC,
    PULSAR_URL,
)

TELEMETRY_SCHEMA = StructType([
    StructField("device_id", StringType(), True),
    StructField("value",     DoubleType(), True),
    StructField("timestamp", StringType(), True),
])

WATERMARK = "2 minutes"
TRIGGER = "10 seconds"


def _now() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _publish(records: list[dict]) -> None:
    if not records:
        return
    client = pulsar.Client(PULSAR_URL, logger=pulsar.ConsoleLogger(pulsar.LoggerLevel.Error))
    producer = client.create_producer(METRICS_TOPIC, send_timeout_millis=0)
    for r in records:
        producer.send(json.dumps(r).encode())
    producer.close()
    client.close()


# --- foreachBatch publishers ---

def publish_temp_metrics(batch_df, _batch_id):
    records = []
    for row in batch_df.collect():
        records.append({
            "metric":       "temp_window",
            "window_start": row["window"]["start"].isoformat(),
            "window_end":   row["window"]["end"].isoformat(),
            "temp_sum":     row["temp_sum"],
            "temp_count":   int(row["temp_count"]),
            "computed_at":  _now(),
        })
    _publish(records)


def publish_production(batch_df, _batch_id):
    records = []
    for row in batch_df.collect():
        records.append({
            "metric":         "production_hourly",
            "hour":           row["window"]["start"].isoformat(),
            "total_sausages": row["total_sausages"],
            "computed_at":    _now(),
        })
    _publish(records)


def publish_compliance(batch_df, _batch_id):
    records = []
    for row in batch_df.collect():
        records.append({
            "metric":       "compliance_window",
            "window_start": row["window"]["start"].isoformat(),
            "window_end":   row["window"]["end"].isoformat(),
            "mean_temp":    row["mean_temp"],
            "computed_at":  _now(),
        })
    _publish(records)


def publish_vibration(batch_df, _batch_id):
    records = []
    for row in batch_df.collect():
        records.append({
            "metric":         "vibration_window",
            "window_start":   row["window"]["start"].isoformat(),
            "window_end":     row["window"]["end"].isoformat(),
            "mean_vibration": row["mean_vibration"],
            "computed_at":    _now(),
        })
    _publish(records)


# --- Main ---

def run() -> None:
    Path(LANDING_DIR).mkdir(parents=True, exist_ok=True)
    Path(CHECKPOINT_DIR).mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("StreamingMetrics")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream
        .format("json")
        .schema(TELEMETRY_SCHEMA)
        .option("maxFilesPerTrigger", "20")
        .load(LANDING_DIR)
    )

    telemetry = (
        raw
        .withColumn("event_time", to_timestamp(col("timestamp")))
        .withWatermark("event_time", WATERMARK)
    )

    cook = telemetry.filter(col("device_id").isin("cook_temp_1", "cook_temp_2"))

    # Metric 1 — sliding mean cook temperature (2-min window, 30-sec slide).
    temp_stream = (
        cook
        .groupBy(window("event_time", "2 minutes", "30 seconds"))
        .agg(
            sum("value").alias("temp_sum"),
            count("value").alias("temp_count"),
        )
    )

    # Metric 2 — hourly sausage production counter.
    sausage_stream = (
        telemetry
        .filter(col("device_id") == "sausage_count")
        .groupBy(window("event_time", "1 hour"))
        .agg(sum("value").alias("total_sausages"))
    )

    # Metric 3 — cook temperature food-safety compliance (2-min tumbling windows).
    compliance_stream = (
        cook
        .groupBy(window("event_time", "2 minutes"))
        .agg((sum("value") / count("value")).alias("mean_temp"))
    )

    # Metric 4 — mixer vibration health (10-min sliding window).
    vibration_stream = (
        telemetry
        .filter(col("device_id") == "mixer_vibration")
        .groupBy(window("event_time", "10 minutes", "1 minute"))
        .agg(avg("value").alias("mean_vibration"))
    )

    checkpoint = lambda name: f"{CHECKPOINT_DIR}/{name}"

    q1 = (
        temp_stream.writeStream
        .foreachBatch(publish_temp_metrics)
        .outputMode("update")
        .option("checkpointLocation", checkpoint("temp"))
        .trigger(processingTime=TRIGGER)
        .start()
    )

    q2 = (
        sausage_stream.writeStream
        .foreachBatch(publish_production)
        .outputMode("update")
        .option("checkpointLocation", checkpoint("production"))
        .trigger(processingTime=TRIGGER)
        .start()
    )

    q3 = (
        compliance_stream.writeStream
        .foreachBatch(publish_compliance)
        .outputMode("update")
        .option("checkpointLocation", checkpoint("compliance"))
        .trigger(processingTime=TRIGGER)
        .start()
    )

    q4 = (
        vibration_stream.writeStream
        .foreachBatch(publish_vibration)
        .outputMode("update")
        .option("checkpointLocation", checkpoint("vibration"))
        .trigger(processingTime="30 seconds")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    run()

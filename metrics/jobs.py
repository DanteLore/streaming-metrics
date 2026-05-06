"""PySpark Structured Streaming jobs.

Reads telemetry directly from the Pulsar telemetry topic via the StreamNative
Pulsar-Spark connector, calculates four metrics with proper watermarking, then
publishes each result back to the Pulsar metrics topic via foreachBatch.

The watermark is set to 2 minutes so that late-arriving events (up to ~90 seconds
late in the lag-simulation scenario) are still included in their correct windows.
When Spark reprocesses a window after late data arrives, an updated message is
published to the metrics topic — this is the eventual-consistency correction.
"""
import json
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, from_json, sum, to_timestamp, window
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from shared import pulsar_streams as streams
from shared.schema import (
    CHECKPOINT_DIR,
    METRICS_TOPIC,
    PULSAR_ADMIN_URL,
    PULSAR_URL,
    TELEMETRY_TOPIC,
)

TELEMETRY_SCHEMA = StructType([
    StructField("device_id", StringType(), True),
    StructField("value",     DoubleType(), True),
    StructField("timestamp", StringType(), True),
])

WATERMARK = "2 minutes"
TRIGGER = "10 seconds"

PULSAR_SPARK_CONNECTOR = "io.streamnative.connectors:pulsar-spark-connector_2.12:3.4.0.4"


def _now() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _publish(records: list[dict]) -> None:
    if not records:
        return
    try:
        client = streams.make_client()
        producer = streams.make_producer(client, METRICS_TOPIC)
        streams.publish_batch(producer, records)
        producer.close()
        client.close()
    except Exception as e:
        print(f"ERROR publishing to metrics topic: {e}")
        raise


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
    Path(CHECKPOINT_DIR).mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("StreamingMetrics")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.jars.packages", PULSAR_SPARK_CONNECTOR)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw_bytes = (
        spark.readStream
        .format("pulsar")
        .option("service.url", PULSAR_URL)
        .option("admin.url", PULSAR_ADMIN_URL)
        .option("topic", TELEMETRY_TOPIC)
        .load()
    )

    raw = raw_bytes.select(
        from_json(col("value").cast("string"), TELEMETRY_SCHEMA).alias("data")
    ).select("data.*")

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

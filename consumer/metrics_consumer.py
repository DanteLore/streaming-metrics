"""Reads computed metrics from the Pulsar metrics topic and writes them to SQLite.

This is the final leg of the pipeline:
  Pulsar [metrics] -> SQLite -> Streamlit UI

Compliance violations are filtered here: only windows where mean cook temperature
falls below FOOD_SAFETY_MIN_TEMP are written to the compliance_violations table.
All other metric types are written unconditionally.
"""
import json

import pulsar

from shared.db import get_conn, init_db
from shared.schema import DB_PATH, FOOD_SAFETY_MIN_TEMP, METRICS_TOPIC, PULSAR_URL

RECEIVE_TIMEOUT_MS = 500


def _handle(conn, msg: dict) -> None:
    metric = msg.get("metric")

    if metric == "temp_window":
        conn.execute(
            "INSERT OR REPLACE INTO temp_metrics "
            "(window_start, window_end, temp_sum, temp_count, computed_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (msg["window_start"], msg["window_end"],
             msg["temp_sum"], msg["temp_count"], msg["computed_at"]),
        )

    elif metric == "production_hourly":
        conn.execute(
            "INSERT OR REPLACE INTO production_hourly (hour, total_sausages, computed_at) "
            "VALUES (?, ?, ?)",
            (msg["hour"], msg["total_sausages"], msg["computed_at"]),
        )

    elif metric == "compliance_window":
        if msg["mean_temp"] < FOOD_SAFETY_MIN_TEMP:
            conn.execute(
                "INSERT OR REPLACE INTO compliance_violations "
                "(window_start, window_end, mean_temp, computed_at) "
                "VALUES (?, ?, ?, ?)",
                (msg["window_start"], msg["window_end"],
                 msg["mean_temp"], msg["computed_at"]),
            )

    elif metric == "vibration_window":
        conn.execute(
            "INSERT OR REPLACE INTO vibration_metrics "
            "(window_start, window_end, mean_vibration, computed_at) "
            "VALUES (?, ?, ?, ?)",
            (msg["window_start"], msg["window_end"],
             msg["mean_vibration"], msg["computed_at"]),
        )


def run() -> None:
    init_db(DB_PATH)

    client = pulsar.Client(PULSAR_URL, logger=pulsar.ConsoleLogger(pulsar.LoggerLevel.Error))
    consumer = client.subscribe(
        METRICS_TOPIC,
        subscription_name="metrics-sqlite-sink",
        consumer_type=pulsar.ConsumerType.Shared,
    )

    print(f"Metrics consumer reading from {METRICS_TOPIC}")

    try:
        while True:
            try:
                msg = consumer.receive(timeout_millis=RECEIVE_TIMEOUT_MS)
                data = json.loads(msg.data().decode())
                with get_conn(DB_PATH) as conn:
                    _handle(conn, data)
                consumer.acknowledge(msg)
            except pulsar.exceptions.Timeout:
                pass

    finally:
        consumer.close()
        client.close()


if __name__ == "__main__":
    run()

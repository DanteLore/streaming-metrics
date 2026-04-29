"""Reads telemetry from Pulsar and fans it out to two sinks:
- data/landing/  — NDJSON files consumed by the Spark streaming job
- SQLite latest_readings table — consumed by the Streamlit UI for the live SCADA view
"""
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import pulsar

from shared.db import get_conn, init_db
from shared.schema import DB_PATH, LANDING_DIR, PULSAR_URL, TELEMETRY_TOPIC

BATCH_SIZE = 10     # write a landing file after accumulating this many messages
MAX_WAIT = 1.0      # …or after this many seconds, whichever comes first
RECEIVE_TIMEOUT_MS = 200


def _write_landing(messages: list[dict]) -> None:
    Path(LANDING_DIR).mkdir(parents=True, exist_ok=True)
    filename = Path(LANDING_DIR) / f"{int(time.time() * 1000)}.json"
    with open(filename, "w") as f:
        for msg in messages:
            f.write(json.dumps(msg) + "\n")


def _update_latest(conn, reading: dict) -> None:
    conn.execute(
        """INSERT OR REPLACE INTO latest_readings (device_id, value, event_time, received_at)
           VALUES (?, ?, ?, ?)""",
        (
            reading["device_id"],
            reading["value"],
            reading["timestamp"],
            datetime.now(tz=timezone.utc).isoformat(),
        ),
    )


def run() -> None:
    init_db(DB_PATH)

    client = pulsar.Client(PULSAR_URL, logger=pulsar.ConsoleLogger(pulsar.LoggerLevel.Error))
    consumer = client.subscribe(
        TELEMETRY_TOPIC,
        subscription_name="metrics-bridge",
        consumer_type=pulsar.ConsumerType.Shared,
    )

    print(f"Bridge consuming from {TELEMETRY_TOPIC}")

    buffer: list[dict] = []
    last_flush = time.time()

    try:
        while True:
            try:
                msg = consumer.receive(timeout_millis=RECEIVE_TIMEOUT_MS)
                reading = json.loads(msg.data().decode())
                buffer.append(reading)
                consumer.acknowledge(msg)
            except pulsar.exceptions.Timeout:
                pass

            now = time.time()
            if buffer and (len(buffer) >= BATCH_SIZE or now - last_flush >= MAX_WAIT):
                _write_landing(buffer)
                with get_conn(DB_PATH) as conn:
                    for r in buffer:
                        _update_latest(conn, r)
                buffer.clear()
                last_flush = now

    finally:
        consumer.close()
        client.close()


if __name__ == "__main__":
    run()

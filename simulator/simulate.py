import json
import math
import random
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pulsar

from shared.schema import PULSAR_URL, TELEMETRY_TOPIC, DEVICES

LAG_FLAG = Path("data/flags/simulate_lag")
PUBLISH_INTERVAL = 1.0  # seconds between full sensor sweeps


def _sensor_value(device_id: str, t: float) -> float:
    """Generate a plausible sensor reading at unix time t using a slow sine wave + noise."""
    d = DEVICES[device_id]
    mid = (d["min"] + d["max"]) / 2.0
    amplitude = (d["max"] - d["min"]) * 0.2
    value = mid + amplitude * math.sin(t / 60.0) + random.gauss(0, amplitude * 0.08)
    return round(max(d["min"] * 0.9, min(d["max"] * 1.1, value)), 3)


def _reading(device_id: str, t: float, ts_override: str | None = None) -> dict:
    ts = ts_override or datetime.fromtimestamp(t, tz=timezone.utc).isoformat()
    return {"device_id": device_id, "value": _sensor_value(device_id, t), "timestamp": ts}


def _publish(producer, reading: dict) -> None:
    producer.send(json.dumps(reading).encode())


def run() -> None:
    client = pulsar.Client(PULSAR_URL, logger=pulsar.ConsoleLogger(pulsar.LoggerLevel.Error))
    producer = client.create_producer(TELEMETRY_TOPIC, send_timeout_millis=0)

    # State for the two eventual-consistency scenarios.
    # Scenario A: sausage_count buffers readings then flushes as a late batch.
    sausage_buffer: list[dict] = []
    flush_after: float | None = None

    # Scenario B: cook_temp_1 sends readings with a deliberate timestamp lag.
    ooo_delay: float = 0.0

    print(f"Simulator publishing to {TELEMETRY_TOPIC}")
    print(f"Sensor lag simulation: toggle by creating/removing {LAG_FLAG}")

    try:
        while True:
            t = time.time()
            lag_active = LAG_FLAG.exists()

            if lag_active and ooo_delay == 0.0:
                ooo_delay = random.uniform(30, 45)
                print(f"Sensor lag ON — cook_temp_1 delay={ooo_delay:.0f}s, sausage_count buffering")
            elif not lag_active and ooo_delay != 0.0:
                ooo_delay = 0.0
                print("Sensor lag OFF")

            for device_id in DEVICES:
                if device_id == "sausage_count":
                    reading = _reading(device_id, t)
                    if lag_active:
                        sausage_buffer.append(reading)
                        if flush_after is None:
                            flush_after = t + random.uniform(60, 90)
                    else:
                        _publish(producer, reading)
                    continue

                if device_id == "cook_temp_1" and ooo_delay:
                    # Send with a stale timestamp so Spark sees out-of-order events.
                    stale_ts = datetime.fromtimestamp(t - ooo_delay, tz=timezone.utc).isoformat()
                    reading = _reading(device_id, t, ts_override=stale_ts)
                else:
                    reading = _reading(device_id, t)

                _publish(producer, reading)

            # Flush buffered sausage counts when the hold period expires.
            if flush_after and t >= flush_after:
                print(f"Flushing {len(sausage_buffer)} buffered sausage_count readings")
                for r in sausage_buffer:
                    _publish(producer, r)
                sausage_buffer.clear()
                flush_after = None

            time.sleep(PUBLISH_INTERVAL)

    finally:
        producer.close()
        client.close()


if __name__ == "__main__":
    run()

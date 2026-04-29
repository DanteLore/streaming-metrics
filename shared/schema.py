PULSAR_URL = "pulsar://localhost:6650"
PULSAR_ADMIN_URL = "http://localhost:8080"
TELEMETRY_TOPIC = "persistent://public/default/telemetry"
METRICS_TOPIC = "persistent://public/default/metrics"

CHECKPOINT_DIR = "data/checkpoints"

# Each device has a stage, display unit, and normal operating range.
# Values outside the range will be flagged in the UI.
DEVICES = {
    "hopper_fill":       {"stage": "hopper",   "unit": "%",     "min": 20.0, "max": 95.0},
    "mixer_rpm":         {"stage": "mixer",    "unit": "RPM",   "min": 60.0, "max": 120.0},
    "mixer_vibration":   {"stage": "mixer",    "unit": "g",     "min": 0.1,  "max": 1.5},
    "extruder_pressure": {"stage": "extruder", "unit": "bar",   "min": 3.0,  "max": 8.0},
    "cook_temp_1":       {"stage": "cooker",   "unit": "°C", "min": 72.0, "max": 95.0},
    "cook_temp_2":       {"stage": "cooker",   "unit": "°C", "min": 72.0, "max": 95.0},
    "steam_pressure":    {"stage": "cooker",   "unit": "bar",   "min": 1.5,  "max": 3.0},
    "belt_speed":        {"stage": "conveyor", "unit": "m/min", "min": 0.5,  "max": 2.0},
    "cooler_temp":       {"stage": "cooler",   "unit": "°C", "min": 2.0,  "max": 8.0},
    "sausage_count":     {"stage": "packager", "unit": "ct",    "min": 0.0,  "max": 10.0},
}

FOOD_SAFETY_MIN_TEMP = 72.0        # cook temp must stay at or above this (°C)
VIBRATION_ALERT_RATIO = 1.5        # alert when 10-min mean > this × hourly baseline
COMPLIANCE_CRITICAL_THRESHOLD = 2  # number of violations before CRITICAL status

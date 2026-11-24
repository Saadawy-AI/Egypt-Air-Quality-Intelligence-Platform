import json
import math
import os
from datetime import datetime, timezone

import paho.mqtt.client as mqtt


METRIC_ALIASES = {
    "pm2_5": "pm25",
    "pm2.5": "pm25",
    "pm_2_5": "pm25",
    "pm_1": "pm1",
    "pm_10": "pm10",
    "temperature_c": "temperature",
    "humidity_percent": "humidity",
}


def build_sensor_message(
    *,
    device_id,
    governorate,
    latitude,
    longitude,
    measurements,
    timestamp=None,
    governorate_ar=None,
    firmware_version=None,
):
    if not device_id or not str(device_id).strip():
        raise ValueError("device_id is required.")
    if not isinstance(measurements, dict) or not measurements:
        raise ValueError("measurements must be a non-empty mapping.")

    latitude = float(latitude)
    longitude = float(longitude)
    if not 21.5 <= latitude <= 32.2:
        raise ValueError("latitude is outside Egypt bounds.")
    if not 24.5 <= longitude <= 37.0:
        raise ValueError("longitude is outside Egypt bounds.")

    normalized = {}
    for raw_name, raw_value in measurements.items():
        name = str(raw_name).strip().lower().replace(" ", "_")
        name = METRIC_ALIASES.get(name, name)
        value = float(raw_value)
        if not math.isfinite(value):
            raise ValueError(f"{raw_name} must be a finite number.")
        if name in {"pm1", "pm25", "pm10"} and value < 0:
            raise ValueError(f"{name} cannot be negative.")
        normalized[name] = str(value)

    if not {"pm1", "pm25", "pm10"}.intersection(normalized):
        raise ValueError("At least one particulate metric is required.")

    if timestamp is None:
        timestamp = datetime.now(timezone.utc)
    if isinstance(timestamp, datetime):
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        timestamp = timestamp.astimezone(timezone.utc).isoformat().replace(
            "+00:00", "Z"
        )

    metadata = {"device_id": str(device_id).strip()}
    if firmware_version:
        metadata["firmware_version"] = str(firmware_version)

    message = {
        "schema_version": "1.0",
        "source": "esp32_pm_sensor",
        "country_code": "EG",
        "governorate": governorate,
        "governorate_ar": governorate_ar,
        "latitude": latitude,
        "longitude": longitude,
        "timestamp": str(timestamp),
        "ingested_at": datetime.now(timezone.utc).isoformat().replace(
            "+00:00", "Z"
        ),
        "data": normalized,
        "metadata": metadata,
    }
    return message


def publish_measurement(message, mqtt_client=None):
    topic = os.getenv(
        "MQTT_SENSOR_TOPIC",
        "egypt/air-quality/sensors",
    )
    host = os.getenv("MQTT_HOST", "localhost")
    port = int(os.getenv("MQTT_PORT", "1883"))
    payload = json.dumps(message, separators=(",", ":"), allow_nan=False)

    owns_client = mqtt_client is None
    client = mqtt_client or mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2
    )

    try:
        if owns_client:
            client.connect(host, port, keepalive=60)
            client.loop_start()

        result = client.publish(topic, payload, qos=1)
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError(f"MQTT publish failed with code {result.rc}.")
        result.wait_for_publish(timeout=10)
        if not result.is_published():
            raise TimeoutError("MQTT publish was not acknowledged.")

        return result.mid
    finally:
        if owns_client:
            client.loop_stop()
            client.disconnect()

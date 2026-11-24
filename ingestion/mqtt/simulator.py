"""Deterministic virtual IoT air-quality device fleet publishing to MQTT.

All emitted records are explicitly labeled as simulated. This service is
opt-in and must never be presented as physical sensor telemetry.
"""
import argparse
import json
import logging
import math
import os
import random
import signal
import time
from datetime import datetime, timezone
from threading import Event

import paho.mqtt.client as mqtt

STOP_EVENT = Event()


def request_shutdown(signum, frame):
    logger.info("Shutdown signal received | signal=%s", signum)
    STOP_EVENT.set()


signal.signal(signal.SIGTERM, request_shutdown)
signal.signal(signal.SIGINT, request_shutdown)


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("IoTSensorSimulator")

TOPIC_PREFIX = os.getenv("MQTT_SIMULATOR_TOPIC_PREFIX", "egypt/air-quality/simulated")
INTERVAL_SECONDS = max(1, int(os.getenv("IOT_SIMULATOR_INTERVAL_SECONDS", "30")))
DEVICE_COUNT = max(1, int(os.getenv("IOT_SIMULATOR_DEVICE_COUNT", "3")))
SEED = int(os.getenv("IOT_SIMULATOR_SEED", "20261124"))

# Representative fixed locations; these are virtual nodes, not physical devices.
SITES = (
    ("Cairo", "القاهرة", 30.0444, 31.2357),
    ("Giza", "الجيزة", 30.0131, 31.2089),
    ("Alexandria", "الإسكندرية", 31.2001, 29.9187),
    ("Mansoura", "المنصورة", 31.0409, 31.3785),
    ("Aswan", "أسوان", 24.0889, 32.8998),
)


def build_payload(device_index, sequence, rng, now=None):
    governorate, governorate_ar, latitude, longitude = SITES[device_index % len(SITES)]
    # Daily cycle + bounded noise creates plausible variation without claiming
    # these values are observations from real environmental sensors.
    phase = (time.time() % 86400) / 86400 * 2 * math.pi
    baseline = 18.0 + 5.0 * math.sin(phase) + device_index * 1.7
    pm25 = max(1.0, baseline + rng.gauss(0, 1.8))
    pm1 = max(0.5, pm25 * 0.55 + rng.gauss(0, 0.5))
    pm10 = max(pm25, pm25 * 1.65 + rng.gauss(0, 2.0))
    timestamp = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    return {
        "schema_version": "1.0",
        "source": "iot_sensor_simulator",
        "country_code": "EG",
        "governorate": governorate,
        "governorate_ar": governorate_ar,
        "latitude": latitude,
        "longitude": longitude,
        "timestamp": timestamp.isoformat().replace("+00:00", "Z"),
        "ingested_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "data": {
            "pm1": round(pm1, 2),
            "pm25": round(pm25, 2),
            "pm10": round(pm10, 2),
            "temperature": round(25 + 7 * math.sin(phase) + rng.gauss(0, 0.7), 2),
            "humidity": round(55 - 12 * math.sin(phase) + rng.gauss(0, 2), 2),
        },
        "metadata": {
            "device_id": f"virtual-air-node-{device_index + 1:03d}",
            "device_type": "virtual_iot_sensor",
            "simulation": "true",
            "simulation_profile": "egypt-urban-air-quality-v1",
            "sequence": str(sequence),
        },
    }


def publish_once(client, rng, sequence):
    sent = 0
    for device_index in range(DEVICE_COUNT):
        payload = build_payload(device_index, sequence, rng)
        topic = f"{TOPIC_PREFIX}/{payload['governorate'].lower()}/{payload['metadata']['device_id']}"
        result = client.publish(
            topic,
            json.dumps(payload, separators=(",", ":"), allow_nan=False),
            qos=1,
        )
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError(f"MQTT publish failed with code {result.rc}")
        result.wait_for_publish(timeout=10)
        if not result.is_published():
            raise TimeoutError("MQTT broker did not acknowledge simulated message")
        sent += 1
        logger.info(
            "Published simulated IoT telemetry | device=%s | sequence=%s | topic=%s",
            payload["metadata"]["device_id"],
            sequence,
            topic,
        )
    return sent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true", help="Publish one message per virtual device and exit.")
    args = parser.parse_args()

    host = os.getenv("MQTT_HOST", "mqtt")
    port = int(os.getenv("MQTT_PORT", "1883"))
    rng = random.Random(SEED)
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=os.getenv("MQTT_SIMULATOR_CLIENT_ID", "egypt-air-quality-iot-simulator"),
    )
    client.connect(host, port, keepalive=60)
    client.loop_start()
    sequence = 0
    total = 0
    try:
        while not STOP_EVENT.is_set():
            sequence += 1
            total += publish_once(client, rng, sequence)
            if args.once:
                break
            STOP_EVENT.wait(INTERVAL_SECONDS)
    except KeyboardInterrupt:
        logger.info("Simulator stopped by operator.")
    finally:
        client.loop_stop()
        client.disconnect()
        logger.info("Simulator finished | total_messages=%s", total)


if __name__ == "__main__":
    main()

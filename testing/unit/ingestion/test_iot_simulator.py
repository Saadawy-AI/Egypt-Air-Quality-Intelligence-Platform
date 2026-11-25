from datetime import datetime, timezone
import random

from ingestion.iot.simulator.simulator import build_payload


def test_simulator_emits_explicitly_labeled_iot_telemetry():
    payload = build_payload(
        0,
        7,
        random.Random(1234),
        now=datetime(2026, 11, 24, 12, 0, tzinfo=timezone.utc),
    )

    assert payload["schema_version"] == "1.0"
    assert payload["source"] == "iot_sensor_simulator"
    assert payload["country_code"] == "EG"
    assert payload["metadata"]["simulation"] == "true"
    assert payload["metadata"]["device_type"] == "virtual_iot_sensor"
    assert payload["metadata"]["device_id"] == "virtual-air-node-001"
    assert payload["metadata"]["sequence"] == "7"
    assert set(("pm1", "pm25", "pm10", "temperature", "humidity")) <= set(payload["data"])
    assert all(float(value) >= 0 for key, value in payload["data"].items() if key in {"pm1", "pm25", "pm10"})


def test_simulator_generates_distinct_virtual_devices():
    first = build_payload(0, 1, random.Random(1))
    second = build_payload(1, 1, random.Random(1))

    assert first["metadata"]["device_id"] != second["metadata"]["device_id"]
    assert first["governorate"] != second["governorate"]
    assert first["source"] == second["source"] == "iot_sensor_simulator"

from ingestion.processors.validator import get_validation_errors


def test_simulator_payload_passes_shared_ingestion_validation():
    payload = build_payload(0, 1, random.Random(99))
    assert get_validation_errors(payload) == []

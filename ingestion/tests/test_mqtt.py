import json

import pytest

from ingestion.mqtt.publisher import build_sensor_message
from ingestion.mqtt.subscriber import MQTTKafkaBridge


class FakeFuture:
    def __init__(self, error=None):
        self.error = error

    def get(self, timeout=None):
        if self.error:
            raise self.error
        return None


class FakeProducer:
    def __init__(self, error=None):
        self.error = error
        self.messages = []

    def send(self, topic, key=None, value=None):
        self.messages.append((topic, key, value))
        return FakeFuture(self.error)


class FakeMQTTClient:
    def __init__(self):
        self.acks = []
        self.subscriptions = []
        self.disconnected = False
        self.on_connect = None
        self.on_disconnect = None
        self.on_message = None

    def manual_ack_set(self, enabled):
        self.manual_ack = enabled

    def subscribe(self, topic, qos=0):
        self.subscriptions.append((topic, qos))
        return 0, 1

    def ack(self, message_id, qos):
        self.acks.append((message_id, qos))
        return 0

    def disconnect(self):
        self.disconnected = True


class FakeMessage:
    topic = "egypt/air-quality/cairo/device-01"
    mid = 17
    qos = 1

    def __init__(self, payload):
        self.payload = payload


def test_build_sensor_message_normalizes_pm25_alias_and_metadata():
    message = build_sensor_message(
        device_id="device-01",
        governorate="Cairo",
        governorate_ar="القاهرة",
        latitude=30.0444,
        longitude=31.2357,
        timestamp="2026-11-23T04:00:00Z",
        measurements={"pm1": 3, "pm2_5": 12.5, "pm10": 20},
        firmware_version="1.2.0",
    )

    assert message["source"] == "esp32_pm_sensor"
    assert message["country_code"] == "EG"
    assert message["data"] == {
        "pm1": "3.0",
        "pm25": "12.5",
        "pm10": "20.0",
    }
    assert message["metadata"]["device_id"] == "device-01"


@pytest.mark.parametrize(
    "overrides",
    [
        {"device_id": ""},
        {"latitude": 40.0},
        {"longitude": 10.0},
        {"measurements": {}},
        {"measurements": {"pm25": -1}},
        {"measurements": {"pm25": float("nan")}},
        {"measurements": {"temperature": 22}},
    ],
)
def test_build_sensor_message_rejects_invalid_sensor_data(overrides):
    arguments = {
        "device_id": "device-01",
        "governorate": "Cairo",
        "latitude": 30.0444,
        "longitude": 31.2357,
        "measurements": {"pm25": 12.5},
    }
    arguments.update(overrides)

    with pytest.raises(ValueError):
        build_sensor_message(**arguments)


def test_mqtt_bridge_subscribes_and_forwards_with_device_key():
    producer = FakeProducer()
    client = FakeMQTTClient()
    bridge = MQTTKafkaBridge(
        producer=producer,
        mqtt_client=client,
        topic_filter="egypt/air-quality/#",
    )

    bridge.on_connect(client, None, None, 0, None)
    payload = json.dumps({
        "metadata": {"device_id": "device-01"},
        "data": {"pm25": "12.5"},
    })
    bridge.on_message(client, None, FakeMessage(payload.encode()))

    assert client.subscriptions == [("egypt/air-quality/#", 1)]
    assert producer.messages == [
        ("air-quality", b"device-01", payload.encode())
    ]
    assert client.acks == [(17, 1)]


def test_mqtt_bridge_forwards_invalid_json_for_silver_quarantine():
    producer = FakeProducer()
    client = FakeMQTTClient()
    bridge = MQTTKafkaBridge(producer=producer, mqtt_client=client)

    payload = b"{not-json"
    bridge.on_message(client, None, FakeMessage(payload))

    assert producer.messages == [
        ("air-quality", None, payload)
    ]
    assert client.acks == [(17, 1)]


def test_mqtt_bridge_forwards_non_object_json_for_silver_quarantine():
    producer = FakeProducer()
    client = FakeMQTTClient()
    bridge = MQTTKafkaBridge(producer=producer, mqtt_client=client)

    payload = b"[]"
    bridge.on_message(client, None, FakeMessage(payload))

    assert producer.messages == [
        ("air-quality", None, payload)
    ]
    assert client.acks == [(17, 1)]


def test_mqtt_bridge_does_not_ack_when_kafka_publish_fails():
    producer = FakeProducer(error=RuntimeError("Kafka unavailable"))
    client = FakeMQTTClient()
    bridge = MQTTKafkaBridge(producer=producer, mqtt_client=client)

    bridge.on_message(
        client,
        None,
        FakeMessage(b'{"metadata":{"device_id":"device-01"}}'),
    )

    assert client.acks == []
    assert client.disconnected

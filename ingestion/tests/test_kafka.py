import pytest

from unittest.mock import MagicMock

from ingestion.producers.kafka_producer import KafkaProducerClient
from ingestion.config.settings import settings


# ==========================================
# FIXTURES
# ==========================================

@pytest.fixture
def kafka_client(monkeypatch):

    mock_producer = MagicMock()

    mock_metadata = MagicMock()
    mock_metadata.topic = "test_topic"
    mock_metadata.partition = 0
    mock_metadata.offset = 10

    mock_future = MagicMock()
    mock_future.get.return_value = mock_metadata

    mock_producer.send.return_value = mock_future

    monkeypatch.setattr(
        "ingestion.producers.kafka_producer.KafkaProducer",
        lambda **kwargs: mock_producer
    )

    client = KafkaProducerClient(
        bootstrap_servers="localhost:9092"
    )

    return client, mock_producer


# ==========================================
# TOPIC ROUTING TESTS
# ==========================================

@pytest.mark.parametrize(
    "source, expected_topic",
    [
        ("openaq", settings.KAFKA_OPENAQ_TOPIC),

        (
            "open_meteo_air_quality",
            settings.KAFKA_AIR_QUALITY_TOPIC
        ),

        (
            "open_meteo_weather",
            settings.KAFKA_WEATHER_TOPIC
        )
    ]
)
def test_get_topic(source, expected_topic):

    record = {"source": source}

    assert (
        KafkaProducerClient.get_topic(record)
        == expected_topic
    )


def test_unsupported_source():

    record = {"source": "unsupported_source"}

    with pytest.raises(ValueError):

        KafkaProducerClient.get_topic(record)


# ==========================================
# RECORD KEY TESTS
# ==========================================

def test_get_record_key():

    record = {
        "source": "openaq",
        "governorate": "Cairo",
        "timestamp": "2026-11-14T12:00:00Z"
    }

    key = KafkaProducerClient.get_record_key(record)

    assert key == (
        "openaq:Cairo:2026-11-14T12:00:00Z"
    )


def test_get_record_key_metadata_fallback():

    record = {
        "source": "openaq",
        "metadata": {
            "location_id": 123
        },
        "timestamp": "2026-11-14T12:00:00Z"
    }

    key = KafkaProducerClient.get_record_key(record)

    assert key == (
        "openaq:123:2026-11-14T12:00:00Z"
    )


# ==========================================
# SEND RECORD TESTS
# ==========================================

def test_send_record_success(kafka_client):

    client, producer = kafka_client

    record = {
        "source": "openaq",
        "governorate": "Cairo",
        "timestamp": "2026-11-14T12:00:00Z"
    }

    metadata = client.send_record(record)

    assert metadata.offset == 10

    producer.send.assert_called_once()

    call_kwargs = producer.send.call_args.kwargs

    assert call_kwargs["value"] == record

    assert call_kwargs["key"] == (
        "openaq:Cairo:2026-11-14T12:00:00Z"
    )


def test_send_record_custom_topic(kafka_client):

    client, producer = kafka_client

    record = {
        "source": "openaq",
        "governorate": "Cairo",
        "timestamp": "2026-11-14T12:00:00Z"
    }

    client.send_record(
        record,
        topic="custom_topic"
    )

    assert (
        producer.send.call_args.args[0]
        == "custom_topic"
    )


def test_send_record_invalid_type(kafka_client):

    client, producer = kafka_client

    with pytest.raises(TypeError):

        client.send_record("invalid_record")

    producer.send.assert_not_called()


# ==========================================
# SEND MULTIPLE RECORDS
# ==========================================

def test_send_records(kafka_client):

    client, producer = kafka_client

    records = [
        {
            "source": "openaq",
            "governorate": "Cairo",
            "timestamp": "2026-11-14T10:00:00Z"
        },

        {
            "source": "openaq",
            "governorate": "Giza",
            "timestamp": "2026-11-14T11:00:00Z"
        }
    ]

    sent = client.send_records(records)

    assert sent == 2

    assert producer.send.call_count == 2


def test_send_records_empty(kafka_client):

    client, producer = kafka_client

    sent = client.send_records([])

    assert sent == 0

    producer.send.assert_not_called()


# ==========================================
# FLUSH AND CLOSE
# ==========================================

def test_flush(kafka_client):

    client, producer = kafka_client

    client.flush()

    producer.flush.assert_called_once()


def test_close(kafka_client):

    client, producer = kafka_client

    client.close()

    producer.flush.assert_called_once()

    producer.close.assert_called_once()


# ==========================================
# CONTEXT MANAGER
# ==========================================

def test_context_manager(monkeypatch):

    mock_producer = MagicMock()

    monkeypatch.setattr(
        "ingestion.producers.kafka_producer.KafkaProducer",
        lambda **kwargs: mock_producer
    )

    with KafkaProducerClient() as client:

        assert isinstance(client, KafkaProducerClient)

    mock_producer.flush.assert_called_once()

    mock_producer.close.assert_called_once()

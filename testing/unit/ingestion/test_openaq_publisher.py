import pytest

from unittest.mock import MagicMock

from kafka.errors import KafkaError

from ingestion.openaq.publisher import (
    OpenAQPublisher
)

from ingestion.config.settings import settings


@pytest.fixture
def kafka_client():

    client = MagicMock()

    metadata = MagicMock()
    metadata.topic = settings.KAFKA_OPENAQ_TOPIC
    metadata.partition = 0
    metadata.offset = 10

    client.send_record.return_value = metadata

    return client


@pytest.fixture
def publisher(kafka_client):

    return OpenAQPublisher(
        producer_client=kafka_client
    )


@pytest.fixture
def records():

    return [
        {
            "source": "openaq",
            "governorate": "Cairo",
            "timestamp": "2026-11-15T10:00:00Z",
            "data": {
                "pollutant": "pm25",
                "value": 35.5
            }
        },
        {
            "source": "openaq",
            "governorate": "Giza",
            "timestamp": "2026-11-15T11:00:00Z",
            "data": {
                "pollutant": "pm10",
                "value": 42.0
            }
        }
    ]


# ==========================================
# INITIALIZATION
# ==========================================

def test_publisher_initialization(kafka_client):

    publisher = OpenAQPublisher(kafka_client)

    assert publisher.producer_client is kafka_client

    assert publisher.topic == (
        settings.KAFKA_OPENAQ_TOPIC
    )


# ==========================================
# SUCCESSFUL PUBLISHING
# ==========================================

def test_publish_success(
    publisher,
    kafka_client,
    records
):

    result = publisher.publish(records)

    assert result["total"] == 2

    assert result["sent"] == 2

    assert result["failed"] == 0

    assert result["success_rate"] == 100.0

    assert result["failures"] == []

    assert kafka_client.send_record.call_count == 2


def test_publish_uses_openaq_topic(
    publisher,
    kafka_client,
    records
):

    publisher.publish(records)

    for call in kafka_client.send_record.call_args_list:

        assert call.kwargs["topic"] == (
            settings.KAFKA_OPENAQ_TOPIC
        )


# ==========================================
# EMPTY RECORDS
# ==========================================

def test_publish_empty_records(publisher, kafka_client):

    result = publisher.publish([])

    assert result["total"] == 0

    assert result["sent"] == 0

    assert result["failed"] == 0

    assert result["success_rate"] == 0.0

    kafka_client.send_record.assert_not_called()


# ==========================================
# PARTIAL FAILURE
# ==========================================

def test_publish_continues_after_failure(
    publisher,
    kafka_client,
    records
):

    kafka_client.send_record.side_effect = [
        MagicMock(),
        KafkaError("Kafka delivery failed")
    ]

    result = publisher.publish(records)

    assert result["total"] == 2

    assert result["sent"] == 1

    assert result["failed"] == 1

    assert result["success_rate"] == 50.0

    assert len(result["failures"]) == 1

    assert kafka_client.send_record.call_count == 2


# ==========================================
# ALL RECORDS FAILED
# ==========================================

def test_publish_all_failed(
    publisher,
    kafka_client,
    records
):

    kafka_client.send_record.side_effect = (
        KafkaError("Kafka unavailable")
    )

    result = publisher.publish(records)

    assert result["total"] == 2

    assert result["sent"] == 0

    assert result["failed"] == 2

    assert result["success_rate"] == 0.0

    assert len(result["failures"]) == 2


# ==========================================
# INVALID RECORDS
# ==========================================

def test_publish_invalid_record(
    publisher,
    kafka_client
):

    result = publisher.publish([
        "invalid_record"
    ])

    assert result["total"] == 1

    assert result["sent"] == 0

    assert result["failed"] == 1

    kafka_client.send_record.assert_not_called()

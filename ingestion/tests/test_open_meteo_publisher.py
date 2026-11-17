import pytest
from types import SimpleNamespace

from ingestion.publishers.open_meteo_publisher import (
    OpenMeteoPublisher
)


@pytest.fixture
def air_quality_record():
    return {
        "source": "open_meteo_air_quality",
        "governorate": "Cairo",
        "timestamp": "2026-11-15T00:00:00Z",
        "data": {
            "pm2_5": 15.5,
            "pm10": 25.3
        }
    }


@pytest.fixture
def weather_record():
    return {
        "source": "open_meteo_weather",
        "governorate": "Cairo",
        "timestamp": "2026-11-15T00:00:00Z",
        "data": {
            "temperature_2m": 22.5,
            "relative_humidity_2m": 55
        }
    }


@pytest.fixture
def mock_producer():
    class MockProducer:

        def __init__(self):
            self.sent_records = []

        def send_record(self, record, topic=None):
            self.sent_records.append({
                "record": record,
                "topic": topic
            })

            return SimpleNamespace(
                topic=(
                    "air-quality"
                    if record["source"] == "open_meteo_air_quality"
                    else "weather"
                ),
                partition=0,
                offset=len(self.sent_records) - 1
            )

    return MockProducer()


def test_publisher_initialization(mock_producer):

    publisher = OpenMeteoPublisher(
        producer_client=mock_producer
    )

    assert publisher.producer_client is mock_producer


def test_publish_air_quality_record(
    mock_producer,
    air_quality_record
):

    publisher = OpenMeteoPublisher(mock_producer)

    result = publisher.publish([air_quality_record])

    assert result["total"] == 1
    assert result["sent"] == 1
    assert result["failed"] == 0
    assert result["success_rate"] == 100.0

    assert len(mock_producer.sent_records) == 1

    assert (
        mock_producer.sent_records[0]["record"]
        == air_quality_record
    )

    assert mock_producer.sent_records[0]["topic"] is None


def test_publish_weather_record(
    mock_producer,
    weather_record
):

    publisher = OpenMeteoPublisher(mock_producer)

    result = publisher.publish([weather_record])

    assert result["total"] == 1
    assert result["sent"] == 1
    assert result["failed"] == 0

    assert (
        mock_producer.sent_records[0]["topic"]
        is None
    )


def test_publish_both_sources(
    mock_producer,
    air_quality_record,
    weather_record
):

    publisher = OpenMeteoPublisher(mock_producer)

    result = publisher.publish([
        air_quality_record,
        weather_record
    ])

    assert result["total"] == 2
    assert result["sent"] == 2
    assert result["failed"] == 0

    assert len(mock_producer.sent_records) == 2

    sources = [
        item["record"]["source"]
        for item in mock_producer.sent_records
    ]

    assert sources == [
        "open_meteo_air_quality",
        "open_meteo_weather"
    ]


def test_reject_openaq_record(
    mock_producer
):

    publisher = OpenMeteoPublisher(mock_producer)

    openaq_record = {
        "source": "openaq",
        "governorate": "Cairo"
    }

    result = publisher.publish([openaq_record])

    assert result["total"] == 1
    assert result["sent"] == 0
    assert result["failed"] == 1

    assert len(mock_producer.sent_records) == 0

    assert "Open-Meteo publisher only accepts" in (
        result["failures"][0]["error"]
    )


def test_reject_invalid_record_type(mock_producer):

    publisher = OpenMeteoPublisher(mock_producer)

    result = publisher.publish([
        "invalid record"
    ])

    assert result["total"] == 1
    assert result["sent"] == 0
    assert result["failed"] == 1

    assert "dictionary" in (
        result["failures"][0]["error"]
    )


def test_partial_failure(
    mock_producer,
    air_quality_record,
    weather_record
):

    original_send = mock_producer.send_record

    def send_with_failure(record, topic=None):

        if record["source"] == "open_meteo_weather":
            raise RuntimeError("Kafka connection error")

        return original_send(record, topic)

    mock_producer.send_record = send_with_failure

    publisher = OpenMeteoPublisher(mock_producer)

    result = publisher.publish([
        air_quality_record,
        weather_record
    ])

    assert result["total"] == 2
    assert result["sent"] == 1
    assert result["failed"] == 1

    assert result["success_rate"] == 50.0

    assert len(result["failures"]) == 1


def test_all_records_failed(mock_producer):

    def failing_send(record, topic=None):
        raise RuntimeError("Kafka unavailable")

    mock_producer.send_record = failing_send

    publisher = OpenMeteoPublisher(mock_producer)

    result = publisher.publish([
        {
            "source": "open_meteo_weather"
        },
        {
            "source": "open_meteo_air_quality"
        }
    ])

    assert result["total"] == 2
    assert result["sent"] == 0
    assert result["failed"] == 2
    assert result["success_rate"] == 0.0

    assert len(result["failures"]) == 2


def test_publish_empty_records(mock_producer):

    publisher = OpenMeteoPublisher(mock_producer)

    result = publisher.publish([])

    assert result == {
        "total": 0,
        "sent": 0,
        "failed": 0,
        "success_rate": 0.0,
        "failures": []
    }

    assert len(mock_producer.sent_records) == 0

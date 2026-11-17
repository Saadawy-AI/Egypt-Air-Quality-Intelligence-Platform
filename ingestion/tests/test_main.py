import pytest

from ingestion import main


class EmptyMeteoClient:
    def get_all_governorates(self):
        return []

    def close(self):
        pass


class EmptyOpenAQClient:
    def get_all_egypt_locations(self):
        return []

    def close(self):
        pass


def install_empty_clients(monkeypatch):
    monkeypatch.setattr(
        main,
        "OpenMeteoAirQualityClient",
        EmptyMeteoClient
    )
    monkeypatch.setattr(
        main,
        "OpenMeteoWeatherClient",
        EmptyMeteoClient
    )
    monkeypatch.setattr(
        main,
        "OpenAQClient",
        EmptyOpenAQClient
    )


def test_run_ingestion_fails_when_no_valid_records(monkeypatch):
    install_empty_clients(monkeypatch)

    with pytest.raises(RuntimeError, match="without any valid records"):
        main.run_ingestion()


def test_run_ingestion_fails_when_kafka_publish_fails(monkeypatch):
    install_empty_clients(monkeypatch)
    monkeypatch.setattr(
        main,
        "fetch_normalized_meteo_records",
        lambda *_: ([{"source": "open_meteo_air_quality"}], [])
    )

    class Producer:
        def close(self):
            pass

    class FailedPublisher:
        def __init__(self, producer_client):
            pass

        def publish(self, records):
            return {
                "total": len(records),
                "sent": 0,
                "failed": len(records),
                "success_rate": 0.0,
                "failures": [{"index": 0, "error": "broker unavailable"}]
            }

    monkeypatch.setattr(main, "KafkaProducerClient", Producer)
    monkeypatch.setattr(main, "OpenMeteoPublisher", FailedPublisher)

    with pytest.raises(RuntimeError, match="failed to publish"):
        main.run_ingestion()

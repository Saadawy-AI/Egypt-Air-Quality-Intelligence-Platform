import pytest

from ingestion.common import open_meteo_pipeline


@pytest.fixture
def air_quality_record():
    return {
        "source": "open_meteo_air_quality",
        "governorate": "Cairo",
        "governorate_ar": "القاهرة",
        "latitude": 30.0444,
        "longitude": 31.2357,
        "data": {
            "hourly": {
                "time": [
                    "2026-11-15T00:00",
                    "2026-11-15T01:00"
                ],
                "pm2_5": [15.5, 18.2],
                "pm10": [25.3, 29.1]
            }
        }
    }


@pytest.fixture
def weather_record():
    return {
        "source": "open_meteo_weather",
        "governorate": "Cairo",
        "governorate_ar": "القاهرة",
        "latitude": 30.0444,
        "longitude": 31.2357,
        "data": {
            "hourly": {
                "time": [
                    "2026-11-15T00:00",
                    "2026-11-15T01:00"
                ],
                "temperature_2m": [22.5, 21.8],
                "relative_humidity_2m": [55, 58],
                "wind_speed_10m": [12.4, 13.1]
            }
        }
    }


def test_process_air_quality_and_weather(
    air_quality_record,
    weather_record
):
    valid_records, rejected_records = (
        open_meteo_pipeline.process_meteo_records([
            air_quality_record,
            weather_record
        ])
    )

    assert len(valid_records) == 4
    assert len(rejected_records) == 0

    sources = {
        record["source"]
        for record in valid_records
    }

    assert sources == {
        "open_meteo_air_quality",
        "open_meteo_weather"
    }


def test_process_valid_air_quality_record(
    air_quality_record
):
    valid_records, rejected_records = (
        open_meteo_pipeline.process_meteo_records([
            air_quality_record
        ])
    )

    assert len(valid_records) == 2
    assert len(rejected_records) == 0

    assert all(
        record["source"] == "open_meteo_air_quality"
        for record in valid_records
    )


def test_process_valid_weather_record(
    weather_record
):
    valid_records, rejected_records = (
        open_meteo_pipeline.process_meteo_records([
            weather_record
        ])
    )

    assert len(valid_records) == 2
    assert len(rejected_records) == 0

    assert all(
        record["source"] == "open_meteo_weather"
        for record in valid_records
    )


def test_reject_invalid_records(air_quality_record):
    air_quality_record["data"]["hourly"]["pm2_5"] = [
        -10,
        18.2
    ]

    valid_records, rejected_records = (
        open_meteo_pipeline.process_meteo_records([
            air_quality_record
        ])
    )

    assert len(valid_records) == 1
    assert len(rejected_records) == 1

    assert rejected_records[0]["errors"]


def test_normalization_exception(
    air_quality_record,
    monkeypatch
):
    def raise_error(record):
        raise ValueError("Normalization test error")

    monkeypatch.setattr(
        open_meteo_pipeline,
        "normalize_meteo_records",
        raise_error
    )

    valid_records, rejected_records = (
        open_meteo_pipeline.process_meteo_records([
            air_quality_record
        ])
    )

    assert len(valid_records) == 0
    assert len(rejected_records) == 1

    assert "Normalization failed" in (
        rejected_records[0]["errors"][0]
    )


def test_fetch_normalized_meteo_records(
    air_quality_record,
    weather_record
):
    class MockAirQualityClient:

        def get_all_governorates(self):
            return [air_quality_record]

    class MockWeatherClient:

        def get_all_governorates(self):
            return [weather_record]

    valid_records, rejected_records = (
        open_meteo_pipeline.fetch_normalized_meteo_records(
            MockAirQualityClient(),
            MockWeatherClient()
        )
    )

    assert len(valid_records) == 4
    assert len(rejected_records) == 0


def test_fetch_empty_results():

    class MockAirQualityClient:

        def get_all_governorates(self):
            return []

    class MockWeatherClient:

        def get_all_governorates(self):
            return []

    valid_records, rejected_records = (
        open_meteo_pipeline.fetch_normalized_meteo_records(
            MockAirQualityClient(),
            MockWeatherClient()
        )
    )

    assert valid_records == []
    assert rejected_records == []

def test_air_quality_failure_does_not_stop_weather(
    weather_record
):

    class FailingAirQualityClient:

        def get_all_governorates(self):
            raise RuntimeError("Air Quality API unavailable")

    class WorkingWeatherClient:

        def get_all_governorates(self):
            return [weather_record]

    valid, rejected = (
        open_meteo_pipeline.fetch_normalized_meteo_records(
            FailingAirQualityClient(),
            WorkingWeatherClient()
        )
    )

    assert len(valid) == 2
    assert len(rejected) == 1

    assert all(
        record["source"] == "open_meteo_weather"
        for record in valid
    )

    assert rejected[0]["source"] == "open_meteo_air_quality"


def test_weather_failure_does_not_stop_air_quality(
    air_quality_record
):

    class WorkingAirQualityClient:

        def get_all_governorates(self):
            return [air_quality_record]

    class FailingWeatherClient:

        def get_all_governorates(self):
            raise RuntimeError("Weather API unavailable")

    valid, rejected = (
        open_meteo_pipeline.fetch_normalized_meteo_records(
            WorkingAirQualityClient(),
            FailingWeatherClient()
        )
    )

    assert len(valid) == 2
    assert len(rejected) == 1

    assert all(
        record["source"] == "open_meteo_air_quality"
        for record in valid
    )

    assert rejected[0]["source"] == "open_meteo_weather"

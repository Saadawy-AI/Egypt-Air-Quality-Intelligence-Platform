import pytest

from ingestion.processors.normalizer import (
    normalize_openaq_measurement,
    normalize_meteo_records
)


def test_openaq_normalization():

    measurement = {
        "value": 35.5,
        "period": {
            "datetimeFrom": {
                "utc": "2026-11-14T12:00:00Z"
            }
        }
    }

    location = {
        "id": 123,
        "name": "Cairo Station",
        "coordinates": {
            "latitude": 30.0444,
            "longitude": 31.2357
        }
    }

    sensor = {
        "id": 456,
        "parameter": {
            "name": "pm25",
            "units": "µg/m³"
        }
    }

    record = normalize_openaq_measurement(
        measurement,
        location,
        sensor,
        governorate="Cairo"
    )

    assert record["source"] == "openaq"
    assert record["governorate"] == "Cairo"
    assert record["governorate_ar"] == "القاهرة"
    assert record["data"]["value"] == 35.5
    assert record["metadata"]["sensor_id"] == 456


def test_meteo_normalization():

    source_record = {
        "source": "open_meteo_air_quality",
        "governorate": "Cairo",
        "governorate_ar": "القاهرة",
        "latitude": 30.0444,
        "longitude": 31.2357,
        "data": {
            "hourly": {
                "time": [
                    "2026-11-14T12:00",
                    "2026-11-14T13:00"
                ],
                "pm2_5": [25.4, 26.1],
                "pm10": [40.2, 42.0]
            }
        }
    }

    records = normalize_meteo_records(source_record)

    assert len(records) == 2
    assert records[0]["data"]["pm2_5"] == 25.4
    assert records[1]["data"]["pm2_5"] == 26.1


def test_meteo_empty_data():

    record = {
        "source": "open_meteo_weather",
        "data": {
            "hourly": {
                "time": []
            }
        }
    }

    assert normalize_meteo_records(record) == []


def test_meteo_preserves_governorate():

    record = {
        "source": "open_meteo_weather",
        "governorate": "Minya",
        "governorate_ar": "المنيا",
        "latitude": 28.1099,
        "longitude": 30.7503,
        "data": {
            "hourly": {
                "time": ["2026-11-14T12:00"],
                "temperature_2m": [25.5]
            }
        }
    }

    result = normalize_meteo_records(record)

    assert result[0]["governorate"] == "Minya"
    assert result[0]["governorate_ar"] == "المنيا"


def test_openaq_latest_normalization():

    measurement = {
        "value": 53.3,
        "datetime": {
            "utc": "2026-11-14T21:00:00Z",
            "local": "2026-11-14T23:00:00+02:00"
        },
        "coordinates": {
            "latitude": 30.0994,
            "longitude": 31.6385
        }
    }

    location = {
        "id": 6528506,
        "name": "Walkthru Earth Main",
        "coordinates": {
            "latitude": 30.0994,
            "longitude": 31.6385
        }
    }

    sensor = {
        "id": 17538372,
        "parameter": {
            "name": "pm25",
            "units": "µg/m³"
        }
    }

    record = normalize_openaq_measurement(
        measurement,
        location,
        sensor,
        governorate="Cairo"
    )

    assert record["source"] == "openaq"

    assert record["timestamp"] == (
        "2026-11-14T21:00:00Z"
    )

    assert record["data"]["pollutant"] == "pm25"

    assert record["data"]["value"] == 53.3

    assert record["data"]["unit"] == "µg/m³"

    assert record["latitude"] == 30.0994

    assert record["longitude"] == 31.6385

    assert record["metadata"]["sensor_id"] == 17538372

    assert record["metadata"]["measurement_source"] == "latest"


def test_openaq_historical_normalization_compatibility():

    measurement = {
        "value": 35.5,
        "period": {
            "datetimeFrom": {
                "utc": "2026-11-14T12:00:00Z",
                "local": "2026-11-14T14:00:00+02:00"
            },
            "datetimeTo": {
                "utc": "2026-11-14T13:00:00Z"
            },
            "interval": "1 hour"
        }
    }

    location = {
        "id": 123,
        "name": "Cairo Station",
        "coordinates": {
            "latitude": 30.0444,
            "longitude": 31.2357
        }
    }

    sensor = {
        "id": 456,
        "parameter": {
            "name": "pm25",
            "units": "µg/m³"
        }
    }

    record = normalize_openaq_measurement(
        measurement,
        location,
        sensor,
        governorate="Cairo"
    )

    assert record["timestamp"] == (
        "2026-11-14T12:00:00Z"
    )

    assert record["metadata"]["datetime_local"] == (
        "2026-11-14T14:00:00+02:00"
    )

    assert record["metadata"]["datetime_to"] == (
        "2026-11-14T13:00:00Z"
    )

    assert record["metadata"]["measurement_source"] == "historical"

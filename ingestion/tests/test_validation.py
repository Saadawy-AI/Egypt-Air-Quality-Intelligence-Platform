import pytest

from ingestion.processors.schema import create_record
from ingestion.processors.validator import (
    validate_record,
    get_validation_errors
)


@pytest.fixture
def valid_meteo_record():

    return create_record(
        source="open_meteo_air_quality",
        latitude=30.0444,
        longitude=31.2357,
        timestamp="2026-11-14T12:00:00",
        governorate="Cairo",
        governorate_ar="القاهرة",
        data={
            "pm2_5": 25.4,
            "pm10": 40.2,
            "ozone": 60.1
        }
    )


def test_valid_record(valid_meteo_record):
    assert validate_record(valid_meteo_record) is True


def test_invalid_source(valid_meteo_record):
    valid_meteo_record["source"] = "unknown_source"

    assert validate_record(valid_meteo_record) is False


def test_invalid_latitude(valid_meteo_record):
    valid_meteo_record["latitude"] = 100

    assert validate_record(valid_meteo_record) is False


def test_coordinates_outside_egypt(valid_meteo_record):
    valid_meteo_record["latitude"] = 40
    valid_meteo_record["longitude"] = 50

    assert validate_record(valid_meteo_record) is False


def test_invalid_governorate(valid_meteo_record):
    valid_meteo_record["governorate"] = "Unknown"

    assert validate_record(valid_meteo_record) is False


def test_invalid_timestamp(valid_meteo_record):
    valid_meteo_record["timestamp"] = "not-a-date"

    assert validate_record(valid_meteo_record) is False


def test_empty_data(valid_meteo_record):
    valid_meteo_record["data"] = {}

    assert validate_record(valid_meteo_record) is False


def test_negative_pollutant(valid_meteo_record):
    valid_meteo_record["data"]["pm2_5"] = -10

    assert validate_record(valid_meteo_record) is False


def test_invalid_metadata(valid_meteo_record):
    valid_meteo_record["metadata"] = []

    assert validate_record(valid_meteo_record) is False


def test_boolean_coordinate(valid_meteo_record):
    valid_meteo_record["latitude"] = True

    assert validate_record(valid_meteo_record) is False


def test_nan_value(valid_meteo_record):
    valid_meteo_record["data"]["pm2_5"] = float("nan")

    assert validate_record(valid_meteo_record) is False


def test_invalid_arabic_governorate_name(valid_meteo_record):
    valid_meteo_record["governorate_ar"] = "الإسكندرية"

    assert validate_record(valid_meteo_record) is False


def test_validation_error_details(valid_meteo_record):
    valid_meteo_record["source"] = "unknown_source"
    valid_meteo_record["timestamp"] = "invalid"

    errors = get_validation_errors(valid_meteo_record)

    assert len(errors) >= 2
    assert any("source" in error.lower() for error in errors)
    assert any("timestamp" in error.lower() for error in errors)


def test_valid_openaq_record():

    record = create_record(
        source="openaq",
        latitude=30.0444,
        longitude=31.2357,
        timestamp="2026-11-14T12:00:00Z",
        governorate="Cairo",
        data={
            "pollutant": "pm25",
            "value": 35.5,
            "unit": "µg/m³"
        },
        metadata={
            "sensor_id": 456,
            "location_id": 123
        }
    )

    assert validate_record(record) is True


def test_negative_openaq_measurement():

    record = create_record(
        source="openaq",
        latitude=30.0444,
        longitude=31.2357,
        timestamp="2026-11-14T12:00:00Z",
        data={
            "pollutant": "pm25",
            "value": -5,
            "unit": "µg/m³"
        }
    )

    assert validate_record(record) is False

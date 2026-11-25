import pytest

from ingestion.openaq import pipeline as openaq_pipeline


class FakeOpenAQClient:

    def __init__(self, sensors, measurements):
        self.sensors = sensors
        self.measurements = measurements

    def get_sensors(self, location_id):
        return self.sensors.get(location_id, [])

    def get_latest_measurements(self, location_id):
        return self.measurements.get(location_id, [])


@pytest.fixture
def location():

    return {
        "id": 100,
        "name": "Cairo Station",
        "coordinates": {
            "latitude": 30.0444,
            "longitude": 31.2357
        }
    }


@pytest.fixture
def sensor():

    return {
        "id": 200,
        "parameter": {
            "name": "pm25",
            "units": "µg/m³"
        }
    }


def test_pipeline_successful_processing(
    monkeypatch,
    location,
    sensor
):

    client = FakeOpenAQClient(
        sensors={100: [sensor]},
        measurements={
            100: [
                {
                    "sensorsId": 200,
                    "value": 35.5
                }
            ]
        }
    )

    def fake_normalizer(measurement, location, sensor):

        return {
            "source": "openaq",
            "data": {
                "pollutant": "pm25",
                "value": measurement["value"]
            }
        }

    monkeypatch.setattr(
        openaq_pipeline,
        "normalize_openaq_measurement",
        fake_normalizer
    )

    monkeypatch.setattr(
        openaq_pipeline,
        "get_validation_errors",
        lambda record: []
    )

    valid, rejected = (
        openaq_pipeline.fetch_normalized_latest_records(
            client,
            [location]
        )
    )

    assert len(valid) == 1
    assert len(rejected) == 0

    assert valid[0]["data"]["value"] == 35.5


def test_pipeline_rejects_unknown_sensor(
    monkeypatch,
    location,
    sensor
):

    client = FakeOpenAQClient(
        sensors={100: [sensor]},
        measurements={
            100: [
                {
                    "sensorsId": 999,
                    "value": 35.5
                }
            ]
        }
    )

    valid, rejected = (
        openaq_pipeline.fetch_normalized_latest_records(
            client,
            [location]
        )
    )

    assert len(valid) == 0
    assert len(rejected) == 1

    assert "Unknown sensor reference: 999" in (
        rejected[0]["errors"]
    )


def test_pipeline_rejects_invalid_record(
    monkeypatch,
    location,
    sensor
):

    client = FakeOpenAQClient(
        sensors={100: [sensor]},
        measurements={
            100: [
                {
                    "sensorsId": 200,
                    "value": -10
                }
            ]
        }
    )

    monkeypatch.setattr(
        openaq_pipeline,
        "normalize_openaq_measurement",
        lambda measurement, location, sensor: {
            "source": "openaq",
            "data": {
                "value": measurement["value"]
            }
        }
    )

    monkeypatch.setattr(
        openaq_pipeline,
        "get_validation_errors",
        lambda record: (
            ["Measurement value cannot be negative"]
            if record["data"]["value"] < 0
            else []
        )
    )

    valid, rejected = (
        openaq_pipeline.fetch_normalized_latest_records(
            client,
            [location]
        )
    )

    assert len(valid) == 0
    assert len(rejected) == 1

    assert (
        "Measurement value cannot be negative"
        in rejected[0]["errors"]
    )


def test_pipeline_skips_location_without_id(
    monkeypatch
):

    client = FakeOpenAQClient(
        sensors={},
        measurements={}
    )

    location_without_id = {
        "name": "Unknown Station"
    }

    valid, rejected = (
        openaq_pipeline.fetch_normalized_latest_records(
            client,
            [location_without_id]
        )
    )

    assert valid == []
    assert rejected == []


def test_pipeline_processes_multiple_measurements(
    monkeypatch,
    location,
    sensor
):

    second_sensor = {
        "id": 201,
        "parameter": {
            "name": "pm1",
            "units": "µg/m³"
        }
    }

    client = FakeOpenAQClient(
        sensors={
            100: [sensor, second_sensor]
        },
        measurements={
            100: [
                {
                    "sensorsId": 200,
                    "value": 35.5
                },
                {
                    "sensorsId": 201,
                    "value": 20.0
                }
            ]
        }
    )

    monkeypatch.setattr(
        openaq_pipeline,
        "normalize_openaq_measurement",
        lambda measurement, location, sensor: {
            "data": {
                "value": measurement["value"],
                "pollutant": sensor["parameter"]["name"]
            }
        }
    )

    monkeypatch.setattr(
        openaq_pipeline,
        "get_validation_errors",
        lambda record: []
    )

    valid, rejected = (
        openaq_pipeline.fetch_normalized_latest_records(
            client,
            [location]
        )
    )

    assert len(valid) == 2
    assert len(rejected) == 0

    assert valid[0]["data"]["pollutant"] == "pm25"
    assert valid[1]["data"]["pollutant"] == "pm1"


def test_pipeline_continues_after_location_failure(
    monkeypatch,
    location,
    sensor
):
    failed_location = {
        "id": 101,
        "name": "Failed Station"
    }

    successful_location = location

    class PartiallyFailingClient:

        def get_sensors(self, location_id):

            if location_id == 101:
                raise RuntimeError("API connection failed")

            return [sensor]

        def get_latest_measurements(self, location_id):
            return [{
                "sensorsId": 200,
                "value": 35.5
            }]

    monkeypatch.setattr(
        openaq_pipeline,
        "normalize_openaq_measurement",
        lambda measurement, location, sensor: {
            "source": "openaq",
            "data": {
                "value": measurement["value"]
            }
        }
    )

    monkeypatch.setattr(
        openaq_pipeline,
        "get_validation_errors",
        lambda record: []
    )

    valid, rejected = (
        openaq_pipeline.fetch_normalized_latest_records(
            PartiallyFailingClient(),
            [failed_location, successful_location]
        )
    )

    assert len(valid) == 1
    assert len(rejected) == 1

    assert valid[0]["data"]["value"] == 35.5

    assert "Location processing failed" in (
        rejected[0]["errors"][0]
    )
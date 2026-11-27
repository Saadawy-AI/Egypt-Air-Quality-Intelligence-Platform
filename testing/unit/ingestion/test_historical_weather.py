import pytest

from ingestion.fetch_historical_weather import HOURLY_VARIABLES, fetch_weather


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, payload):
        self.payload = payload
        self.calls = 0

    def get(self, url, params, timeout):
        self.calls += 1
        assert params["models"] == "best_match"
        assert params["timezone"] == "UTC"
        return FakeResponse(self.payload)


def sample_payload():
    times = ["2026-09-07T00:00", "2026-09-07T01:00"]
    hourly = {"time": times}
    for variable in HOURLY_VARIABLES:
        hourly[variable] = [1.0, 2.0]
    return {"hourly": hourly, "latitude": 30.0, "longitude": 31.0,
            "elevation": 22, "timezone": "GMT", "hourly_units": {}}


def test_fetch_weather_returns_aligned_hourly_reanalysis():
    session = FakeSession(sample_payload())
    frame, metadata = fetch_weather("Cairo", "2026-09-07", "2026-09-07", session)
    assert len(frame) == 2
    assert frame["window_start"].is_monotonic_increasing
    assert frame["governorate"].unique().tolist() == ["Cairo"]
    assert metadata["dataset"] == "best_match historical reanalysis"
    assert metadata["license"] == "CC BY 4.0"
    assert metadata["rows"] == 2


def test_fetch_weather_rejects_unknown_governorate():
    with pytest.raises(ValueError, match="Unsupported governorate"):
        fetch_weather("Unknown", "2026-09-07", "2026-09-07", FakeSession(sample_payload()))


def test_fetch_weather_rejects_misaligned_variables():
    payload = sample_payload()
    payload["hourly"]["wind_speed_10m"] = [1.0]
    with pytest.raises(RuntimeError, match="Missing or misaligned"):
        fetch_weather("Cairo", "2026-09-07", "2026-09-07", FakeSession(payload))


def test_fetch_weather_rejects_reversed_date_range():
    with pytest.raises(ValueError, match="end_date"):
        fetch_weather("Cairo", "2026-09-08", "2026-09-07", FakeSession(sample_payload()))

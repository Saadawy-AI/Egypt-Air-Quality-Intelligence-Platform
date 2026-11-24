from unittest.mock import Mock

import pytest

from ingestion.sources.openaq import (
    OpenAQClient,
    OpenAQTransientError,
)


def test_get_rejects_non_object_json_payload():
    client = OpenAQClient.__new__(OpenAQClient)
    client.base_url = "https://api.openaq.org/v3"
    client.timeout = 10
    response = Mock(status_code=200)
    response.json.return_value = []
    client.session = Mock()
    client.session.get.return_value = response

    with pytest.raises(OpenAQTransientError, match="invalid JSON payload"):
        OpenAQClient._get.__wrapped__(client, "/locations")

    client.session.get.assert_called_once()


def test_get_hourly_measurements_uses_preferred_hours_endpoint(monkeypatch):
    client = OpenAQClient.__new__(OpenAQClient)
    captured = {}

    def fake_get_all_pages(endpoint, params):
        captured["endpoint"] = endpoint
        captured["params"] = params
        return [{"value": 12.5}]

    monkeypatch.setattr(client, "_get_all_pages", fake_get_all_pages)

    result = client.get_hourly_measurements(
        sensor_id=123,
        date_from="2026-11-01T00:00:00Z",
        date_to="2026-11-02T00:00:00Z",
    )

    assert result == [{"value": 12.5}]
    assert captured == {
        "endpoint": "/sensors/123/hours",
        "params": {
            "datetime_from": "2026-11-01T00:00:00Z",
            "datetime_to": "2026-11-02T00:00:00Z",
        },
    }

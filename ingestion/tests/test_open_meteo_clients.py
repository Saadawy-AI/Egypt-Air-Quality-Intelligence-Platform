import requests
import pytest

from ingestion.sources import open_meteo_air_quality, open_meteo_weather


@pytest.mark.parametrize(
    ("module", "client_class", "method_name", "message"),
    [
        (
            open_meteo_air_quality,
            open_meteo_air_quality.OpenMeteoAirQualityClient,
            "get_governorate_data",
            "Open-Meteo Air Quality fetch failed",
        ),
        (
            open_meteo_weather,
            open_meteo_weather.OpenMeteoWeatherClient,
            "get_governorate_weather",
            "Open-Meteo Weather fetch failed",
        ),
    ],
)
def test_governorate_fetch_failure_is_not_silently_ignored(
    monkeypatch,
    module,
    client_class,
    method_name,
    message,
):
    monkeypatch.setattr(
        module,
        "GOVERNORATES",
        {
            "Cairo": {"name_ar": "القاهرة"},
            "Giza": {"name_ar": "الجيزة"},
        },
    )
    client = object.__new__(client_class)

    def fetch(governorate):
        if governorate == "Giza":
            raise requests.exceptions.Timeout("simulated timeout")
        return {"governorate": governorate}

    monkeypatch.setattr(client, method_name, fetch)

    with pytest.raises(RuntimeError, match=message) as error:
        client.get_all_governorates()

    assert "Giza" in str(error.value)
    assert "simulated timeout" in str(error.value)

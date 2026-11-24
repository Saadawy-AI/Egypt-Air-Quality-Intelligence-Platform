import numpy as np
import pandas as pd

from ml.experiment_pm25_weather import (
    WEATHER_FEATURE_COLUMNS,
    build_lagged_weather_features,
    build_weather_model_frame,
)


def sample_weather(hours=240):
    times = pd.date_range("2026-09-01", periods=hours, freq="h", tz="UTC")
    data = {"window_start": times, "governorate": ["Cairo"] * hours}
    for column in (
        "temperature_2m", "relative_humidity_2m", "dew_point_2m",
        "precipitation", "surface_pressure", "wind_speed_10m",
        "cloud_cover", "wind_direction_10m",
    ):
        data[column] = np.arange(hours, dtype=float)
    return pd.DataFrame(data)


def test_lagged_weather_features_never_use_target_hour_weather():
    weather = sample_weather(10)
    result = build_lagged_weather_features(weather).sort_values("window_start")
    assert len(result) == 10
    assert result.iloc[0][WEATHER_FEATURE_COLUMNS].isna().all()
    assert result.iloc[1]["weather_prev1h_temperature_2m"] == 0.0
    assert result.iloc[2]["weather_prev1h_temperature_2m"] == 1.0
    assert result.iloc[3]["weather_prev3h_mean_temperature_2m"] == 1.0


def test_weather_features_join_only_matching_series_and_utc_hours():
    times = pd.date_range("2026-09-01", periods=420, freq="h", tz="UTC")
    measurements = pd.DataFrame({
        "window_start": times,
        "governorate": ["Cairo"] * len(times),
        "average_value": np.linspace(5, 30, len(times)),
        "measurement_count": [1] * len(times),
    })
    weather = sample_weather(420)
    frame = build_weather_model_frame(measurements, weather)
    assert len(frame) > 100
    assert set(WEATHER_FEATURE_COLUMNS).issubset(frame.columns)
    assert frame["series"].unique().tolist() == ["Cairo"]
    # The first target hour with complete weather features is at least t-3h.
    assert frame["window_start"].min() >= times[3]
    assert frame["naive_prediction"].notna().all()

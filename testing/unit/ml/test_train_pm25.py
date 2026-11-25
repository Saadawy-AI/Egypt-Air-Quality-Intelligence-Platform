import numpy as np
import pandas as pd

from ml.training.train_pm25 import (
    FEATURE_COLUMNS,
    build_series_features,
    chronological_split,
)


def test_features_use_only_prior_observations():
    times = pd.date_range("2026-01-01", periods=220, freq="h", tz="UTC")
    frame = pd.DataFrame({
        "window_start": times,
        "governorate": "Cairo",
        "average_value": np.arange(220, dtype=float),
        "measurement_count": 1,
    })

    result = build_series_features(frame, series_code=0)
    row = result.loc[result["window_start"] == times[168]].iloc[0]

    assert row["lag_1"] == 167.0
    assert row["lag_24"] == 144.0
    assert row["rolling_mean_24"] == np.mean(np.arange(144, 168))
    assert row["target"] == 168.0
    assert list(FEATURE_COLUMNS) == [
        "lag_1", "lag_2", "lag_3", "lag_6", "lag_12", "lag_24",
        "lag_48", "lag_168", "rolling_mean_24", "rolling_std_24",
        "rolling_mean_168", "hour_sin", "hour_cos", "weekday_sin",
        "weekday_cos", "month_sin", "month_cos", "series_code",
    ]


def test_missing_hours_are_not_interpolated_into_lags():
    times = pd.date_range("2026-01-01", periods=220, freq="h", tz="UTC")
    frame = pd.DataFrame({
        "window_start": times.delete(100),
        "governorate": "Cairo",
        "average_value": np.delete(np.arange(220, dtype=float), 100),
        "measurement_count": 1,
    })

    result = build_series_features(frame, series_code=0).set_index("window_start")

    assert times[101] not in result.index
    assert times[102] not in result.index


def test_train_test_split_is_global_and_strictly_chronological():
    times = pd.date_range("2026-01-01", periods=200, freq="h", tz="UTC")
    frame = pd.DataFrame({
        "window_start": list(times) + list(times),
        "series": ["Cairo"] * len(times) + ["Giza"] * len(times),
    })

    train, test = chronological_split(frame, train_fraction=0.8)

    assert train["window_start"].max() < test["window_start"].min()

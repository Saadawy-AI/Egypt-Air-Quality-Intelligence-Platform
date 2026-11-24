"""Inspect PM2.5 / lagged weather alignment without mutating project data."""
import json
from pathlib import Path

import pandas as pd

from ml.experiment_pm25 import latest_contiguous_regime
from ml.experiment_pm25_weather import WEATHER_FEATURE_COLUMNS, build_lagged_weather_features
from ml.train_pm25 import build_feature_frame, load_measurements


def main():
    measurements = load_measurements()
    series_counts = measurements.groupby("governorate").size()
    eligible_series = series_counts[series_counts >= 168].index
    measurements = measurements[measurements["governorate"].isin(eligible_series)].copy()
    measurements, regime = latest_contiguous_regime(
        measurements, series_column="governorate"
    )
    base = build_feature_frame(measurements)
    weather_path = Path("/opt/project-data/staging/research/openaq_weather_reanalysis_cairo_best_match_20260907_20261118.csv")
    weather = pd.read_csv(weather_path)
    weather_features = build_lagged_weather_features(weather)
    merged = base.merge(
        weather_features, on=["window_start", "series"], how="inner"
    )
    complete = merged.dropna(subset=WEATHER_FEATURE_COLUMNS)
    print(json.dumps({
        "measurements": {
            "rows": len(measurements),
            "governorates": sorted(measurements.governorate.unique().tolist()),
            "first": measurements.window_start.min().isoformat(),
            "last": measurements.window_start.max().isoformat(),
        },
        "pm_features": {
            "rows": len(base),
            "first": base.window_start.min().isoformat(),
            "last": base.window_start.max().isoformat(),
            "timestamp_dtype": str(base.window_start.dtype),
        },
        "weather": {
            "rows": len(weather),
            "governorates": sorted(weather.governorate.unique().tolist()),
            "first": str(weather.window_start.min()),
            "last": str(weather.window_start.max()),
        },
        "weather_features": {
            "rows": len(weather_features),
            "first": weather_features.window_start.min().isoformat(),
            "last": weather_features.window_start.max().isoformat(),
            "timestamp_dtype": str(weather_features.window_start.dtype),
            "null_counts": weather_features[WEATHER_FEATURE_COLUMNS].isna().sum().to_dict(),
        },
        "joined_rows": len(merged),
        "complete_weather_rows": len(complete),
        "regime": regime,
    }, indent=2, default=str))


if __name__ == "__main__":
    main()

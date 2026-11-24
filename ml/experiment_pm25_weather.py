"""Research-only PM2.5 experiment with lagged ERA5-Land weather covariates."""
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from ml.experiment_pm25 import (
    CANDIDATES,
    build_estimator,
    latest_contiguous_regime,
    metric_bundle,
    select_candidate,
    summarize_fold_metrics,
    time_series_folds,
)
from ml.train_pm25 import (
    FEATURE_COLUMNS,
    build_feature_frame,
    chronological_split,
    load_measurements,
)

WEATHER_COLUMNS = [
    "temperature_2m", "relative_humidity_2m", "dew_point_2m",
    "precipitation", "surface_pressure", "wind_speed_10m",
    "cloud_cover", "wind_direction_sin", "wind_direction_cos",
]
WEATHER_FEATURE_COLUMNS = [f"weather_prev1h_{name}" for name in WEATHER_COLUMNS] + [
    f"weather_prev3h_mean_{name}" for name in WEATHER_COLUMNS
]


def build_lagged_weather_features(weather):
    """Use only weather known at or before t-1 for a target at time t."""
    frame = weather.copy()
    frame["window_start"] = pd.to_datetime(frame["window_start"], utc=True)
    frame = frame.sort_values(["governorate", "window_start"])
    direction = np.deg2rad(pd.to_numeric(frame["wind_direction_10m"], errors="coerce"))
    frame["wind_direction_sin"] = np.sin(direction)
    frame["wind_direction_cos"] = np.cos(direction)
    frames = []
    for governorate, group in frame.groupby("governorate"):
        group = group.drop_duplicates("window_start", keep="last").set_index("window_start")
        hourly_index = pd.date_range(
            group.index.min().floor("h"), group.index.max().floor("h"),
            freq="h", tz="UTC",
        )
        hourly = group.reindex(hourly_index)
        features = pd.DataFrame(index=hourly_index)
        for column in WEATHER_COLUMNS:
            values = pd.to_numeric(hourly[column], errors="coerce")
            features[f"weather_prev1h_{column}"] = values.shift(1)
            features[f"weather_prev3h_mean_{column}"] = (
                values.shift(1).rolling(3, min_periods=3).mean()
            )
        features["window_start"] = hourly_index
        features["series"] = governorate
        frames.append(features.reset_index(drop=True))
    if not frames:
        raise RuntimeError("No historical weather features could be constructed.")
    return pd.concat(frames, ignore_index=True)


def build_weather_model_frame(measurements, weather):
    """Join PM2.5 targets to weather strictly by governorate/hour."""
    base = build_feature_frame(measurements)
    weather_features = build_lagged_weather_features(weather)
    merged = base.merge(
        weather_features,
        on=["window_start", "series"],
        how="inner",
        validate="one_to_one",
    )
    required = FEATURE_COLUMNS + WEATHER_FEATURE_COLUMNS + ["target", "naive_prediction"]
    merged = merged.replace([np.inf, -np.inf], np.nan).dropna(subset=required)
    if merged.empty:
        raise RuntimeError("No PM2.5 rows have complete lagged historical weather features.")
    merged.attrs["raw_rows"] = int(len(measurements))
    merged.attrs["weather_input_rows"] = int(len(weather))
    merged.attrs["weather_joined_rows_before_feature_drop"] = int(len(base.merge(
        weather_features, on=["window_start", "series"], how="inner"
    )))
    return merged.sort_values(["window_start", "series"]).reset_index(drop=True)


def run_weather_cv(training_frame, feature_columns):
    results = {}
    for name, spec in CANDIDATES.items():
        fold_metrics = []
        baseline_metrics = []
        for train, valid in time_series_folds(training_frame):
            model = build_estimator(spec)
            model.fit(train[feature_columns], train["target"])
            prediction = model.predict(valid[feature_columns])
            fold_metrics.append(metric_bundle(valid["target"], prediction))
            baseline_metrics.append(metric_bundle(valid["target"], valid["naive_prediction"]))
        cv = summarize_fold_metrics(fold_metrics)
        baseline = summarize_fold_metrics(baseline_metrics)
        results[name] = {
            "model_class": spec["model_class"],
            "parameters": spec["parameters"],
            "folds": fold_metrics,
            "cv": cv,
            "persistence_cv": baseline,
            "cv_gate_passed": (
                cv["mean_mae"] < baseline["mean_mae"]
                and cv["mean_rmse"] < baseline["mean_rmse"]
            ),
        }
    return results


def run_weather_experiment(frame, output_dir):
    feature_columns = FEATURE_COLUMNS + WEATHER_FEATURE_COLUMNS
    train, holdout = chronological_split(frame)
    cv_results = run_weather_cv(train, feature_columns)
    selected = select_candidate(cv_results)
    spec = {
        "model_class": cv_results[selected]["model_class"],
        "parameters": cv_results[selected]["parameters"],
    }
    model = build_estimator(spec)
    model.fit(train[feature_columns], train["target"])
    prediction = model.predict(holdout[feature_columns])
    model_metrics = metric_bundle(holdout["target"], prediction)
    baseline_metrics = metric_bundle(holdout["target"], holdout["naive_prediction"])
    cv_gate = bool(cv_results[selected]["cv_gate_passed"])
    holdout_gate = (
        model_metrics["mae"] < baseline_metrics["mae"]
        and model_metrics["rmse"] < baseline_metrics["rmse"]
    )
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    version = "pm25_weather_reanalysis_v1"
    model_path = output / f"{version}.joblib"
    metadata_path = output / f"{version}.json"
    predictions_path = output / f"{version}_holdout_predictions.csv"
    details = pd.DataFrame({
        "window_start": holdout["window_start"].to_numpy(),
        "series": holdout["series"].astype(str).to_numpy(),
        "actual": holdout["target"].astype(float).to_numpy(),
        "model_prediction": prediction,
        "persistence_prediction": holdout["naive_prediction"].astype(float).to_numpy(),
    })
    details["model_error"] = details["actual"] - details["model_prediction"]
    details["persistence_error"] = details["actual"] - details["persistence_prediction"]
    metadata = {
        "experiment_version": version,
        "model_class": spec["model_class"],
        "selected_candidate": selected,
        "cv_results": cv_results,
        "cv_gate_passed": cv_gate,
        "holdout": {
            "model": model_metrics,
            "persistence_baseline": baseline_metrics,
            "baseline_gate_passed": holdout_gate,
            "rows": int(len(holdout)),
            "start": holdout["window_start"].min().isoformat(),
            "end": holdout["window_start"].max().isoformat(),
            "prediction_artifact": predictions_path.name,
        },
        "training": {
            "rows": int(len(train)),
            "start": train["window_start"].min().isoformat(),
            "end": train["window_start"].max().isoformat(),
            "raw_pm_measurements": int(frame.attrs.get("raw_rows", 0)),
            "weather_rows_downloaded": int(frame.attrs.get("weather_input_rows", 0)),
            "joined_rows_before_feature_drop": int(frame.attrs.get("weather_joined_rows_before_feature_drop", 0)),
            "model_rows_after_lag_alignment": int(len(frame)),
            "series": sorted(frame["series"].unique().tolist()),
        },
        "feature_columns": feature_columns,
        "weather": {
            "provider": "Open-Meteo Historical Weather API",
            "dataset": "ERA5-Land reanalysis",
            "license": "CC BY 4.0",
            "attribution": "Weather data by Open-Meteo.com",
            "features_use_only_weather_at_t_minus_1_and_prior_3h": True,
            "target_hour_weather_used": False,
            "forecast_vintage_available": False,
            "research_only": True,
            "caveat": "Reanalysis is modelled historical weather, not a local station observation or archived forecast vintage.",
        },
        "target": "actual OpenAQ hourly PM2.5 average (µg/m³)",
        "prediction_horizon": "one-hour-ahead rolling evaluation; weather covariates lagged at least one hour",
        "production_deployed": False,
        "candidate_status": (
            "qualified_for_review" if cv_gate and holdout_gate
            else "not_qualified_cv_or_holdout_gate_failed"
        ),
        "overall_qualification_gate_passed": bool(cv_gate and holdout_gate),
        "trained_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    model_tmp = output / f".{version}.joblib.tmp"
    metadata_tmp = output / f".{version}.json.tmp"
    predictions_tmp = output / f".{version}_holdout_predictions.csv.tmp"
    joblib.dump(model, model_tmp)
    metadata_tmp.write_text(json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8")
    details.to_csv(predictions_tmp, index=False)
    model_tmp.replace(model_path)
    metadata_tmp.replace(metadata_path)
    predictions_tmp.replace(predictions_path)
    return metadata


def main():
    measurements = load_measurements()
    series_counts = measurements.groupby("governorate").size()
    eligible_series = series_counts[series_counts >= 168].index
    measurements = measurements[measurements["governorate"].isin(eligible_series)].copy()
    measurements, regime = latest_contiguous_regime(
        measurements, series_column="governorate"
    )
    weather_path = Path(os.getenv(
        "ML_HISTORICAL_WEATHER_PATH",
        "/opt/project-data/staging/research/openaq_weather_reanalysis_cairo_best_match_20260907_20261118.csv",
    ))
    if not weather_path.is_file():
        raise FileNotFoundError(f"Historical weather research file not found: {weather_path}")
    weather = pd.read_csv(weather_path)
    weather_metadata_path = weather_path.with_suffix(".json")
    weather_metadata = (
        json.loads(weather_metadata_path.read_text(encoding="utf-8"))
        if weather_metadata_path.is_file() else {}
    )
    frame = build_weather_model_frame(measurements, weather)
    metadata = run_weather_experiment(
        frame,
        os.getenv(
            "ML_WEATHER_EXPERIMENT_OUTPUT_DIR",
            "/opt/project-models/pm25/experiments/weather_reanalysis",
        ),
    )
    metadata["data_regime"] = "latest_contiguous_openaq_pm25_regime"
    metadata["regime_coverage"] = regime
    metadata["weather"]["input_file"] = weather_path.name
    metadata["weather"]["dataset"] = weather_metadata.get(
        "dataset", "Open-Meteo historical weather reanalysis"
    )
    metadata["weather"]["retrieval_metadata"] = weather_metadata
    metadata["weather"]["time_alignment"] = "exact UTC hour; weather features shifted to t-1 before joining"
    metadata_path = Path(os.getenv(
        "ML_WEATHER_EXPERIMENT_OUTPUT_DIR",
        "/opt/project-models/pm25/experiments/weather_reanalysis",
    )) / "pm25_weather_reanalysis_v1.json"
    metadata_path.write_text(json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps({
        "experiment_version": metadata["experiment_version"],
        "selected_candidate": metadata["selected_candidate"],
        "cv_gate_passed": metadata["cv_gate_passed"],
        "holdout": metadata["holdout"],
        "training": metadata["training"],
        "candidate_status": metadata["candidate_status"],
        "model_path": str(metadata_path.with_suffix(".joblib")),
        "metadata_path": str(metadata_path),
    }, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()

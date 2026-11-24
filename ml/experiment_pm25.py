"""Training-only time-series CV and locked-holdout experiments for PM2.5.

This research runner never changes the production model alias and never deploys.
"""
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import (
    ExtraTreesRegressor,
    HistGradientBoostingRegressor,
    RandomForestRegressor,
)
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import TimeSeriesSplit

from ml.train_pm25 import (
    FEATURE_COLUMNS,
    build_feature_frame,
    chronological_split,
    load_measurements,
)

CANDIDATES = {
    "hgb_current_reference": {
        "model_class": "HistGradientBoostingRegressor",
        "parameters": {
            "max_iter": 300, "learning_rate": 0.05,
            "max_leaf_nodes": 15, "l2_regularization": 1.0,
        },
    },
    "hgb_regularized_shallow": {
        "model_class": "HistGradientBoostingRegressor",
        "parameters": {
            "max_iter": 250, "learning_rate": 0.05,
            "max_leaf_nodes": 7, "l2_regularization": 5.0,
        },
    },
    "hgb_higher_capacity": {
        "model_class": "HistGradientBoostingRegressor",
        "parameters": {
            "max_iter": 350, "learning_rate": 0.04,
            "max_leaf_nodes": 31, "l2_regularization": 2.0,
        },
    },
    "random_forest_regularized": {
        "model_class": "RandomForestRegressor",
        "parameters": {
            "n_estimators": 160, "max_depth": 12,
            "min_samples_leaf": 3, "max_features": 0.8, "n_jobs": 1,
        },
    },
    "extra_trees_regularized": {
        "model_class": "ExtraTreesRegressor",
        "parameters": {
            "n_estimators": 160, "max_depth": 12,
            "min_samples_leaf": 3, "max_features": 0.8, "n_jobs": 1,
        },
    },
}
CV_SPLITS = 4


def build_estimator(spec):
    model_class = spec["model_class"]
    parameters = dict(spec["parameters"])
    parameters["random_state"] = 42
    if model_class == "HistGradientBoostingRegressor":
        parameters["early_stopping"] = False
        return HistGradientBoostingRegressor(**parameters)
    if model_class == "RandomForestRegressor":
        return RandomForestRegressor(**parameters)
    if model_class == "ExtraTreesRegressor":
        return ExtraTreesRegressor(**parameters)
    raise ValueError(f"Unsupported experiment model class: {model_class}")


def metric_bundle(actual, predicted):
    return {
        "mae": float(mean_absolute_error(actual, predicted)),
        "rmse": float(mean_squared_error(actual, predicted) ** 0.5),
        "r2": float(r2_score(actual, predicted)),
    }


def time_series_folds(frame, n_splits=CV_SPLITS):
    """Split unique timestamps so rows from one hour never cross a fold boundary."""
    timestamps = np.array(sorted(pd.to_datetime(frame["window_start"], utc=True).unique()))
    if len(timestamps) <= n_splits:
        raise RuntimeError("Not enough unique timestamps for time-series cross-validation.")
    splitter = TimeSeriesSplit(n_splits=n_splits)
    folds = []
    for train_idx, valid_idx in splitter.split(timestamps):
        train_end = timestamps[train_idx[-1]]
        valid_start = timestamps[valid_idx[0]]
        valid_end = timestamps[valid_idx[-1]]
        train = frame[frame["window_start"] <= train_end].copy()
        valid = frame[
            (frame["window_start"] >= valid_start)
            & (frame["window_start"] <= valid_end)
        ].copy()
        if train.empty or valid.empty or train["window_start"].max() >= valid["window_start"].min():
            raise RuntimeError("A time-series CV fold is empty or crosses its time boundary.")
        folds.append((train, valid))
    return folds


def summarize_fold_metrics(fold_metrics):
    summary = {}
    for key in ("mae", "rmse", "r2"):
        values = [item[key] for item in fold_metrics]
        summary[f"mean_{key}"] = float(np.mean(values))
        summary[f"std_{key}"] = float(np.std(values, ddof=1)) if len(values) > 1 else 0.0
    return summary


def run_cross_validation(training_frame, candidates=CANDIDATES, n_splits=CV_SPLITS):
    folds = time_series_folds(training_frame, n_splits=n_splits)
    results = {}
    for name, spec in candidates.items():
        fold_metrics = []
        baseline_metrics = []
        for train, valid in folds:
            estimator = build_estimator(spec)
            estimator.fit(train[FEATURE_COLUMNS], train["target"])
            prediction = estimator.predict(valid[FEATURE_COLUMNS])
            fold_metrics.append(metric_bundle(valid["target"], prediction))
            baseline_metrics.append(
                metric_bundle(valid["target"], valid["naive_prediction"])
            )
        results[name] = {
            "model_class": spec["model_class"],
            "parameters": spec["parameters"],
            "folds": fold_metrics,
            "cv": summarize_fold_metrics(fold_metrics),
            "persistence_cv": summarize_fold_metrics(baseline_metrics),
        }
        results[name]["cv_gate_passed"] = (
            results[name]["cv"]["mean_mae"] < results[name]["persistence_cv"]["mean_mae"]
            and results[name]["cv"]["mean_rmse"] < results[name]["persistence_cv"]["mean_rmse"]
        )
    return results


def select_candidate(cv_results):
    """Choose on training-only CV RMSE, with MAE as deterministic tie-break."""
    return min(
        cv_results,
        key=lambda name: (
            cv_results[name]["cv"]["mean_rmse"],
            cv_results[name]["cv"]["mean_mae"],
        ),
    )


def summarize_temporal_coverage(frame):
    """Describe observation gaps without filling or interpolating missing hours."""
    summaries = []
    for series_name, group in frame.groupby("series"):
        timestamps = pd.DatetimeIndex(
            pd.to_datetime(group["window_start"], utc=True).drop_duplicates().sort_values()
        )
        gaps = timestamps.to_series().diff().dropna()
        gap_hours = gaps.dt.total_seconds().div(3600)
        large_gaps = gap_hours[gap_hours > 24]
        longest = sorted(gap_hours.items(), key=lambda item: item[1], reverse=True)[:10]
        summaries.append({
            "series": str(series_name),
            "feature_rows": int(len(group)),
            "unique_timestamps": int(len(timestamps)),
            "first_observation": timestamps.min().isoformat(),
            "last_observation": timestamps.max().isoformat(),
            "median_interval_hours": (
                float(gap_hours.median()) if not gap_hours.empty else None
            ),
            "maximum_gap_hours": (
                float(gap_hours.max()) if not gap_hours.empty else 0.0
            ),
            "gaps_over_24h": int(len(large_gaps)),
            "gaps_over_7d": int((gap_hours > 168).sum()),
            "longest_gaps": [
                {
                    "gap_start": (pd.Timestamp(str(end)).to_pydatetime() - timedelta(seconds=int(round(duration_hours * 3600)))).isoformat(),
                    "gap_end": pd.Timestamp(str(end)).isoformat(),
                    "duration_hours": float(duration_hours),
                }
                for end, duration_hours in longest if duration_hours > 24
            ],
        })
    return summaries


def latest_contiguous_regime(frame, gap_threshold_hours=24, series_column="series"):
    """Keep the latest raw-observation segment independently per series."""
    retained = []
    coverage = []
    for series_name, group in frame.groupby(series_column):
        ordered = group.sort_values("window_start").copy()
        timestamps = pd.DatetimeIndex(
            pd.to_datetime(ordered["window_start"], utc=True).drop_duplicates()
        )
        gaps = timestamps.to_series().diff()
        gap_hours = gaps.dt.total_seconds().div(3600)
        boundaries = gap_hours[gap_hours > gap_threshold_hours]
        if boundaries.empty:
            segment_start = timestamps.min()
            preceding_gap_hours = None
        else:
            segment_start = boundaries.index[-1]
            preceding_gap_hours = float(boundaries.iloc[-1])
        segment = ordered[ordered["window_start"] >= segment_start].copy()
        if segment.empty:
            continue
        retained.append(segment)
        coverage.append({
            "series": str(series_name),
            "segment_start": pd.Timestamp(segment["window_start"].min()).isoformat(),
            "segment_end": pd.Timestamp(segment["window_start"].max()).isoformat(),
            "input_rows": int(len(ordered)),
            "retained_rows": int(len(segment)),
            "preceding_gap_hours": preceding_gap_hours,
        })
    if not retained:
        raise RuntimeError("No rows remain in the latest contiguous observation regime.")
    result = pd.concat(retained, ignore_index=True).sort_values(
        ["window_start", series_column]
    )
    result.attrs.update(frame.attrs)
    if result["window_start"].nunique() <= CV_SPLITS + 1:
        raise RuntimeError("Latest contiguous regime is too short for time-series CV.")
    return result, coverage


def build_holdout_diagnostics(holdout, prediction):
    """Return row-level holdout predictions and grouped error diagnostics."""
    diagnostics = pd.DataFrame({
        "window_start": pd.to_datetime(holdout["window_start"], utc=True).to_numpy(),
        "series": holdout["series"].astype(str).to_numpy(),
        "actual": holdout["target"].astype(float).to_numpy(),
        "model_prediction": np.asarray(prediction, dtype=float),
        "persistence_prediction": holdout["naive_prediction"].astype(float).to_numpy(),
    })
    diagnostics["model_error"] = diagnostics["actual"] - diagnostics["model_prediction"]
    diagnostics["model_absolute_error"] = diagnostics["model_error"].abs()
    diagnostics["persistence_error"] = (
        diagnostics["actual"] - diagnostics["persistence_prediction"]
    )
    diagnostics["persistence_absolute_error"] = diagnostics["persistence_error"].abs()
    diagnostics["hour_of_day_utc"] = diagnostics["window_start"].dt.hour
    diagnostics["month_utc"] = diagnostics["window_start"].dt.strftime("%Y-%m")

    hourly = diagnostics.groupby("hour_of_day_utc").agg(
        rows=("actual", "size"),
        model_mae=("model_absolute_error", "mean"),
        model_rmse=("model_error", lambda values: float(np.sqrt(np.mean(np.square(values))))),
        persistence_mae=("persistence_absolute_error", "mean"),
        persistence_rmse=("persistence_error", lambda values: float(np.sqrt(np.mean(np.square(values))))),
    ).reset_index()
    monthly = diagnostics.groupby("month_utc").agg(
        rows=("actual", "size"),
        model_mae=("model_absolute_error", "mean"),
        model_rmse=("model_error", lambda values: float(np.sqrt(np.mean(np.square(values))))),
        persistence_mae=("persistence_absolute_error", "mean"),
        persistence_rmse=("persistence_error", lambda values: float(np.sqrt(np.mean(np.square(values))))),
        mean_signed_error=("model_error", "mean"),
    ).reset_index()
    series = diagnostics.groupby("series").agg(
        rows=("actual", "size"),
        model_mae=("model_absolute_error", "mean"),
        model_rmse=("model_error", lambda values: float(np.sqrt(np.mean(np.square(values))))),
        persistence_mae=("persistence_absolute_error", "mean"),
        persistence_rmse=("persistence_error", lambda values: float(np.sqrt(np.mean(np.square(values))))),
    ).reset_index()

    summary = {
        "absolute_error_p50": float(diagnostics["model_absolute_error"].quantile(0.50)),
        "absolute_error_p90": float(diagnostics["model_absolute_error"].quantile(0.90)),
        "absolute_error_p95": float(diagnostics["model_absolute_error"].quantile(0.95)),
        "mean_signed_error": float(diagnostics["model_error"].mean()),
        "by_hour_of_day_utc": hourly.to_dict(orient="records"),
        "by_month_utc": monthly.to_dict(orient="records"),
        "by_series": series.to_dict(orient="records"),
    }
    return diagnostics, summary


def run_experiment(frame, output_dir, experiment_version="pm25_v3_candidate", extra_metadata=None):
    train, holdout = chronological_split(frame)
    cv_results = run_cross_validation(train)
    selected_name = select_candidate(cv_results)
    selected_result = cv_results[selected_name]
    selected_spec = {
        "model_class": selected_result["model_class"],
        "parameters": selected_result["parameters"],
    }

    final_model = build_estimator(selected_spec)
    final_model.fit(train[FEATURE_COLUMNS], train["target"])
    holdout_prediction = final_model.predict(holdout[FEATURE_COLUMNS])
    holdout_model = metric_bundle(holdout["target"], holdout_prediction)
    holdout_persistence = metric_bundle(
        holdout["target"], holdout["naive_prediction"]
    )
    holdout_diagnostics, diagnostic_summary = build_holdout_diagnostics(
        holdout, holdout_prediction
    )
    holdout_gate = (
        holdout_model["mae"] < holdout_persistence["mae"]
        and holdout_model["rmse"] < holdout_persistence["rmse"]
    )
    cv_gate = bool(cv_results[selected_name]["cv_gate_passed"])
    overall_gate = cv_gate and holdout_gate

    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    model_path = output / f"{experiment_version}.joblib"
    metadata_path = output / f"{experiment_version}.json"
    predictions_path = output / f"{experiment_version}_holdout_predictions.csv"
    temp_model = output / f".{experiment_version}.joblib.tmp"
    temp_metadata = output / f".{experiment_version}.json.tmp"
    temp_predictions = output / f".{experiment_version}_holdout_predictions.csv.tmp"

    metadata = {
        "experiment_version": experiment_version,
        "model_class": selected_spec["model_class"],
        "selected_candidate": selected_name,
        "selected_parameters": selected_spec["parameters"],
        "selection_rule": "Lowest mean RMSE across training-only expanding-window CV; MAE breaks ties.",
        "cv_split_count": len(next(iter(cv_results.values()))["folds"]),
        "cv_results": cv_results,
        "cv_gate_passed": cv_gate,
        "overall_qualification_gate_passed": overall_gate,
        "holdout": {
            "model": holdout_model,
            "persistence_baseline": holdout_persistence,
            "baseline_gate_passed": holdout_gate,
            "rows": int(len(holdout)),
            "start": holdout["window_start"].min().isoformat(),
            "end": holdout["window_start"].max().isoformat(),
            "diagnostics": diagnostic_summary,
            "prediction_artifact": predictions_path.name,
        },
        "temporal_coverage": summarize_temporal_coverage(frame),
        "training": {
            "rows": int(len(train)),
            "start": train["window_start"].min().isoformat(),
            "end": train["window_start"].max().isoformat(),
            "raw_measurement_rows": int(frame.attrs.get("raw_rows", 0)),
            "feature_rows": int(len(frame)),
            "series": sorted(frame["series"].unique().tolist()),
        },
        "feature_columns": FEATURE_COLUMNS,
        "target": "actual OpenAQ hourly PM2.5 average (µg/m³)",
        "prediction_horizon": "one-hour-ahead rolling evaluation",
        "production_deployed": False,
        "candidate_status": (
            "qualified_for_review" if overall_gate
            else "not_qualified_cv_or_holdout_gate_failed"
        ),
        "trained_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    if extra_metadata:
        metadata.update(extra_metadata)

    joblib.dump(final_model, temp_model)
    temp_metadata.write_text(
        json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    holdout_diagnostics.to_csv(temp_predictions, index=False)
    temp_model.replace(model_path)
    temp_metadata.replace(metadata_path)
    temp_predictions.replace(predictions_path)
    return metadata, model_path, metadata_path


def main():
    measurements = load_measurements()
    latest_regime = os.getenv("ML_EXPERIMENT_LATEST_REGIME", "false").lower() == "true"
    if latest_regime:
        measurements, regime_coverage = latest_contiguous_regime(
            measurements, series_column="governorate"
        )
    features = build_feature_frame(measurements)
    features.attrs["raw_rows"] = int(len(measurements))
    if latest_regime:
        experiment_version = os.getenv("ML_EXPERIMENT_VERSION", "pm25_v4_recent_regime")
        output_dir = os.getenv(
            "ML_EXPERIMENT_OUTPUT_DIR",
            "/opt/project-models/pm25/experiments/recent_regime",
        )
        extra_metadata = {
            "data_regime": "latest_contiguous_segment_per_series",
            "gap_threshold_hours": 24,
            "regime_coverage": regime_coverage,
            "input_measurement_rows_before_regime_filter": int(sum(
                item["input_rows"] for item in regime_coverage
            )),
            "measurement_rows_after_regime_filter": int(len(measurements)),
            "feature_rows_after_feature_engineering": int(len(features)),
        }
    else:
        experiment_version = os.getenv("ML_EXPERIMENT_VERSION", "pm25_v3_candidate")
        output_dir = os.getenv(
            "ML_EXPERIMENT_OUTPUT_DIR", "/opt/project-models/pm25/experiments"
        )
        extra_metadata = None
    metadata, model_path, metadata_path = run_experiment(
        features, output_dir, experiment_version, extra_metadata
    )
    print(json.dumps({
        "experiment_version": metadata["experiment_version"],
        "selected_candidate": metadata["selected_candidate"],
        "cv_results": metadata["cv_results"],
        "holdout": metadata["holdout"],
        "candidate_status": metadata["candidate_status"],
        "data_regime": metadata.get("data_regime", "full_history"),
        "regime_coverage": metadata.get("regime_coverage"),
        "model_path": str(model_path),
        "metadata_path": str(metadata_path),
    }, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()

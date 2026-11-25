import numpy as np
import pandas as pd

from ml.training.experiment_pm25 import (
    build_holdout_diagnostics,
    build_estimator,
    latest_contiguous_regime,
    summarize_temporal_coverage,
    select_candidate,
    summarize_fold_metrics,
    time_series_folds,
)
from sklearn.ensemble import ExtraTreesRegressor, HistGradientBoostingRegressor, RandomForestRegressor


def sample_frame(hours=120):
    times = pd.date_range("2026-01-01", periods=hours, freq="h", tz="UTC")
    return pd.DataFrame({
        "window_start": list(times) + list(times),
        "series": ["Cairo"] * hours + ["Giza"] * hours,
        "target": np.arange(hours, dtype=float).tolist() * 2,
        "naive_prediction": (np.arange(hours, dtype=float) - 1).tolist() * 2,
    })


def test_time_series_folds_keep_each_timestamp_in_one_partition():
    frame = sample_frame()
    folds = time_series_folds(frame, n_splits=4)
    assert len(folds) == 4
    for train, valid in folds:
        assert train["window_start"].max() < valid["window_start"].min()
        assert set(train["window_start"]).isdisjoint(set(valid["window_start"]))
        assert train["series"].nunique() == 2
        assert valid["series"].nunique() == 2


def test_cv_candidate_selection_uses_rmse_then_mae():
    results = {
        "higher_mae": {"cv": {"mean_rmse": 3.0, "mean_mae": 2.0}},
        "best_rmse": {"cv": {"mean_rmse": 2.0, "mean_mae": 1.8}},
        "tie_rmse_lower_mae": {"cv": {"mean_rmse": 2.0, "mean_mae": 1.7}},
    }
    assert select_candidate(results) == "tie_rmse_lower_mae"


def test_fold_summary_reports_mean_and_sample_standard_deviation():
    summary = summarize_fold_metrics([
        {"mae": 1.0, "rmse": 2.0, "r2": 0.5},
        {"mae": 3.0, "rmse": 4.0, "r2": 0.7},
    ])
    assert summary["mean_mae"] == 2.0
    assert summary["mean_rmse"] == 3.0
    assert summary["mean_r2"] == 0.6
    assert summary["std_mae"] == 2 ** 0.5


def test_estimator_factory_supports_three_model_families():
    specs = [
        {"model_class": "HistGradientBoostingRegressor", "parameters": {"max_iter": 2}},
        {"model_class": "RandomForestRegressor", "parameters": {"n_estimators": 2}},
        {"model_class": "ExtraTreesRegressor", "parameters": {"n_estimators": 2}},
    ]
    estimators = [build_estimator(spec) for spec in specs]
    assert isinstance(estimators[0], HistGradientBoostingRegressor)
    assert isinstance(estimators[1], RandomForestRegressor)
    assert isinstance(estimators[2], ExtraTreesRegressor)


def test_holdout_diagnostics_report_error_quantiles_and_hourly_baseline():
    times = pd.date_range("2026-02-01", periods=2, freq="h", tz="UTC")
    holdout = pd.DataFrame({
        "window_start": times,
        "series": ["Cairo", "Cairo"],
        "target": [12.0, 10.0],
        "naive_prediction": [11.0, 11.0],
    })
    details, summary = build_holdout_diagnostics(holdout, np.array([10.0, 12.0]))
    assert details["model_absolute_error"].tolist() == [2.0, 2.0]
    assert summary["absolute_error_p50"] == 2.0
    assert summary["absolute_error_p90"] == 2.0
    assert summary["mean_signed_error"] == 0.0
    assert len(summary["by_hour_of_day_utc"]) == 2
    assert summary["by_month_utc"][0]["month_utc"] == "2026-02"
    assert summary["by_month_utc"][0]["rows"] == 2
    assert summary["by_series"][0]["series"] == "Cairo"
    assert summary["by_series"][0]["rows"] == 2


def test_temporal_coverage_reports_large_gaps_without_interpolation():
    times = pd.to_datetime([
        "2026-01-01T00:00:00Z",
        "2026-01-01T01:00:00Z",
        "2026-01-11T01:00:00Z",
    ])
    frame = pd.DataFrame({"window_start": times, "series": ["Cairo"] * 3})
    summary = summarize_temporal_coverage(frame)[0]
    assert summary["feature_rows"] == 3
    assert summary["gaps_over_24h"] == 1
    assert summary["gaps_over_7d"] == 1
    assert summary["maximum_gap_hours"] == 240.0
    assert summary["longest_gaps"][0]["duration_hours"] == 240.0


def test_latest_contiguous_regime_excludes_stale_history_per_series():
    first = pd.date_range("2024-01-01", periods=100, freq="h", tz="UTC")
    recent = pd.date_range("2026-09-01", periods=120, freq="h", tz="UTC")
    frame = pd.DataFrame({
        "window_start": first.append(recent),
        "series": ["Cairo"] * (len(first) + len(recent)),
        "target": range(len(first) + len(recent)),
    })
    latest, coverage = latest_contiguous_regime(frame)
    assert len(latest) == 120
    assert latest["window_start"].min() == recent.min()
    assert coverage[0]["preceding_gap_hours"] > 24
    assert coverage[0]["input_rows"] == 220
    assert coverage[0]["retained_rows"] == 120


def test_latest_contiguous_regime_keeps_series_with_no_large_gap():
    times = pd.date_range("2026-01-01", periods=100, freq="h", tz="UTC")
    frame = pd.DataFrame({"window_start": times, "series": ["Cairo"] * len(times)})
    latest, coverage = latest_contiguous_regime(frame)
    assert len(latest) == 100
    assert coverage[0]["preceding_gap_hours"] is None


def test_latest_contiguous_regime_accepts_raw_governorate_column():
    old = pd.date_range("2025-01-01", periods=50, freq="h", tz="UTC")
    recent = pd.date_range("2026-09-01", periods=80, freq="h", tz="UTC")
    frame = pd.DataFrame({
        "window_start": old.append(recent).append(pd.DatetimeIndex([recent[-1]])),
        "governorate": ["Cairo"] * (len(old) + len(recent)) + ["Alexandria"],
    })
    latest, coverage = latest_contiguous_regime(frame, series_column="governorate")
    assert len(latest[latest["governorate"] == "Cairo"]) == 80
    assert len(latest[latest["governorate"] == "Alexandria"]) == 1
    assert {item["series"] for item in coverage} == {"Cairo", "Alexandria"}

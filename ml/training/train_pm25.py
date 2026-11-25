"""Train/evaluate a one-step-ahead model from actual OpenAQ PM2.5 observations."""
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

SQL = """
SELECT f.window_start, g.governorate_name, f.average_value, f.measurement_count
FROM fact_hourly_measurements AS f
JOIN dim_source AS s ON s.source_key = f.source_key
JOIN dim_governorate AS g ON g.governorate_key = f.governorate_key
JOIN dim_metric AS m ON m.metric_key = f.metric_key
WHERE s.source_name = 'openaq'
  AND m.metric_name = 'pm25'
  AND g.governorate_name <> 'Unknown'
  AND f.average_value IS NOT NULL
  AND f.average_value >= 0
ORDER BY g.governorate_name, f.window_start
"""

LAGS = (1, 2, 3, 6, 12, 24, 48, 168)
FEATURE_COLUMNS = [
    *(f"lag_{lag}" for lag in LAGS),
    "rolling_mean_24", "rolling_std_24", "rolling_mean_168",
    "hour_sin", "hour_cos", "weekday_sin", "weekday_cos",
    "month_sin", "month_cos", "series_code",
]


def connection_string():
    required = ("PGHOST", "PGDATABASE", "PGUSER", "PGPASSWORD")
    missing = [key for key in required if not os.getenv(key)]
    if missing:
        raise RuntimeError("Missing PostgreSQL settings: " + ", ".join(missing))
    return " ".join((
        f"host={os.environ['PGHOST']}",
        f"port={os.getenv('PGPORT', '5432')}",
        f"dbname={os.environ['PGDATABASE']}",
        f"user={os.environ['PGUSER']}",
        f"password={os.environ['PGPASSWORD']}",
    ))


def load_measurements():
    import psycopg

    with psycopg.connect(connection_string()) as conn:
        rows = conn.execute(SQL).fetchall()
    frame = pd.DataFrame(
        rows,
        columns=["window_start", "governorate", "average_value", "measurement_count"],
    )
    if frame.empty:
        raise RuntimeError("No actual OpenAQ PM2.5 observations are available.")
    frame["window_start"] = pd.to_datetime(frame["window_start"], utc=True)
    frame["average_value"] = pd.to_numeric(frame["average_value"], errors="coerce")
    frame["measurement_count"] = pd.to_numeric(
        frame["measurement_count"], errors="coerce"
    )
    frame["governorate"] = frame["governorate"].fillna("Unknown")
    frame = frame.dropna(
        subset=["window_start", "average_value", "measurement_count"]
    )
    return frame[frame["average_value"] >= 0].sort_values(
        ["governorate", "window_start"]
    )


def build_series_features(series_frame, series_code):
    """Create lags/rolling statistics using only observations before target time."""
    series_frame = series_frame.sort_values("window_start").drop_duplicates(
        "window_start", keep="last"
    ).set_index("window_start")
    index = pd.date_range(
        series_frame.index.min().floor("h"),
        series_frame.index.max().floor("h"),
        freq="h",
        tz="UTC",
    )
    hourly = series_frame.reindex(index)
    target = hourly["average_value"]
    features = pd.DataFrame(index=index)

    for lag in LAGS:
        features[f"lag_{lag}"] = target.shift(lag)

    prior = target.shift(1)
    features["rolling_mean_24"] = prior.rolling(24, min_periods=18).mean()
    features["rolling_std_24"] = prior.rolling(24, min_periods=18).std()
    features["rolling_mean_168"] = prior.rolling(168, min_periods=120).mean()

    hour, weekday, month = index.hour, index.dayofweek, index.month - 1
    features["hour_sin"] = np.sin(2 * np.pi * hour / 24)
    features["hour_cos"] = np.cos(2 * np.pi * hour / 24)
    features["weekday_sin"] = np.sin(2 * np.pi * weekday / 7)
    features["weekday_cos"] = np.cos(2 * np.pi * weekday / 7)
    features["month_sin"] = np.sin(2 * np.pi * month / 12)
    features["month_cos"] = np.cos(2 * np.pi * month / 12)
    features["series_code"] = float(series_code)

    result = features.copy()
    result["target"] = target
    result["naive_prediction"] = features["lag_1"]
    result = result.replace([np.inf, -np.inf], np.nan).dropna(
        subset=FEATURE_COLUMNS + ["target", "naive_prediction"]
    )
    result["series"] = str(series_frame["governorate"].iloc[0])
    result.index.name = "window_start"
    return result.reset_index()


def build_feature_frame(measurements):
    names = sorted(measurements["governorate"].unique())
    frames = [
        build_series_features(
            measurements[measurements["governorate"] == name], code
        )
        for code, name in enumerate(names)
    ]
    frames = [frame for frame in frames if not frame.empty]
    if not frames:
        raise RuntimeError("Insufficient contiguous history for lag features.")
    return pd.concat(frames, ignore_index=True).sort_values(
        ["window_start", "series"]
    )


def chronological_split(frame, train_fraction=0.8):
    ordered = frame.sort_values("window_start").reset_index(drop=True)
    split = int(len(ordered) * train_fraction)
    if split < 100 or len(ordered) - split < 30:
        raise RuntimeError("Not enough rows for chronological train/test evaluation.")
    cutoff = ordered.loc[split, "window_start"]
    train = ordered[ordered["window_start"] < cutoff].copy()
    test = ordered[ordered["window_start"] >= cutoff].copy()
    if train.empty or test.empty:
        raise RuntimeError("Chronological split produced an empty train or test set.")
    return train, test


def metrics(actual, predicted):
    return {
        "mae": float(mean_absolute_error(actual, predicted)),
        "rmse": float(mean_squared_error(actual, predicted) ** 0.5),
        "r2": float(r2_score(actual, predicted)),
    }


def train_and_evaluate(frame):
    train, test = chronological_split(frame)
    model = HistGradientBoostingRegressor(
        max_iter=300,
        learning_rate=0.05,
        max_leaf_nodes=15,
        l2_regularization=1.0,
        early_stopping=False,
        random_state=42,
    )
    model.fit(train[FEATURE_COLUMNS], train["target"])
    predicted = model.predict(test[FEATURE_COLUMNS])
    baseline = test["naive_prediction"].to_numpy()
    model_metrics = metrics(test["target"], predicted)
    baseline_metrics = metrics(test["target"], baseline)
    return model, {
        "model": model_metrics,
        "naive_persistence_baseline": baseline_metrics,
        "baseline_gate_passed": (
            model_metrics["mae"] < baseline_metrics["mae"]
            and model_metrics["rmse"] < baseline_metrics["rmse"]
        ),
        "train_rows": int(len(train)),
        "test_rows": int(len(test)),
        "train_start": train["window_start"].min().isoformat(),
        "train_end": train["window_start"].max().isoformat(),
        "test_start": test["window_start"].min().isoformat(),
        "test_end": test["window_start"].max().isoformat(),
        "series": sorted(frame["series"].unique().tolist()),
    }


def main():
    measurements = load_measurements()
    features = build_feature_frame(measurements)
    model, evaluation = train_and_evaluate(features)

    output = Path(os.getenv("ML_OUTPUT_DIR", "/opt/project-models/pm25"))
    output.mkdir(parents=True, exist_ok=True)
    model_path = output / "pm25_hgb_v2.joblib"
    metadata_path = output / "pm25_hgb_v2.json"
    temp_model = output / ".pm25_hgb_v2.joblib.tmp"
    temp_metadata = output / ".pm25_hgb_v2.json.tmp"

    joblib.dump(model, temp_model)
    metadata = {
        "model_version": "pm25_hgb_v2",
        "model_class": "HistGradientBoostingRegressor",
        "target": "actual OpenAQ hourly PM2.5 average (µg/m³)",
        "source_filter": "source_name='openaq' AND metric_name='pm25' AND governorate_name<>'Unknown'",
        "geographic_scope": "Known governorates only; current historical coverage is Cairo.",
        "unknown_governorate_rows_excluded": True,
        "forecast_rows_excluded": True,
        "prediction_horizon": "one-hour-ahead rolling evaluation",
        "feature_columns": FEATURE_COLUMNS,
        "raw_rows": int(len(measurements)),
        "feature_rows": int(len(features)),
        "first_observation_at": measurements["window_start"].min().isoformat(),
        "latest_observation_at": measurements["window_start"].max().isoformat(),
        "gaps_reindexed_without_interpolation": True,
        "production_deployed": False,
        "candidate_status": (
            "qualified_for_review" if evaluation["baseline_gate_passed"]
            else "not_qualified_baseline_gate_failed"
        ),
        "evaluation": evaluation,
        "trained_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    temp_metadata.write_text(
        json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    temp_model.replace(model_path)
    temp_metadata.replace(metadata_path)
    print(json.dumps({
        "raw_rows": int(len(measurements)),
        "feature_rows": int(len(features)),
        "model_path": str(model_path),
        "metadata_path": str(metadata_path),
        "evaluation": evaluation,
    }, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()

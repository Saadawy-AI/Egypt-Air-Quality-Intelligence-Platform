import math
import os
import statistics
from datetime import timedelta

LOOKBACK_DAYS = max(2, int(os.getenv("ANOMALY_LOOKBACK_DAYS", "8")))
MIN_BASELINE_POINTS = max(6, int(os.getenv("ANOMALY_MIN_BASELINE_POINTS", "12")))
MAD_MULTIPLIER = max(2.0, float(os.getenv("ANOMALY_MAD_MULTIPLIER", "3.5")))
RELATIVE_FLOOR = max(0.05, float(os.getenv("ANOMALY_RELATIVE_FLOOR", "0.25")))

# The alerting scope is intentionally limited to pollutant concentrations.
# Weather covariates and UV index have strong diurnal/seasonal cycles and are
# not valid candidates for this generic rolling-baseline detector.
POLLUTANT_METRICS = frozenset({
    "pm1", "pm25", "pm2_5", "pm10", "dust",
    "carbon_monoxide", "nitrogen_dioxide", "ozone", "sulphur_dioxide",
})

# Open-Meteo air-quality concentrations are reported in µg/m³. Absolute
# floors prevent near-zero nighttime baselines from producing enormous scores.
ABSOLUTE_FLOORS = {
    "pm1": 2.0, "pm25": 5.0, "pm2_5": 5.0, "pm10": 10.0, "dust": 5.0,
    "carbon_monoxide": 100.0, "nitrogen_dioxide": 10.0,
    "ozone": 10.0, "sulphur_dioxide": 5.0,
}


def metric_floor(metric, median):
    name = (metric or "").lower()
    return max(ABSOLUTE_FLOORS.get(name, 1e-6), abs(median) * RELATIVE_FLOOR)


def evaluate_series(rows):
    """Compare the latest pollutant aggregate with a robust historical baseline."""
    if not rows:
        return None
    latest = rows[-1]
    metric = (latest.get("metric_name") or "").lower()
    if metric not in POLLUTANT_METRICS:
        return None
    if len(rows) < MIN_BASELINE_POINTS + 1:
        return None
    if latest["average_value"] is None:
        return None
    observed = float(latest["average_value"])
    if not math.isfinite(observed):
        return None

    cutoff = latest["window_start"] - timedelta(days=LOOKBACK_DAYS)
    baseline = [
        float(row["average_value"]) for row in rows[:-1]
        if row["window_start"] >= cutoff
        and row["average_value"] is not None
        and math.isfinite(float(row["average_value"]))
    ]
    if len(baseline) < MIN_BASELINE_POINTS:
        return None

    center = statistics.median(baseline)
    mad = statistics.median(abs(value - center) for value in baseline)
    robust_sigma = 1.4826 * mad
    floor = metric_floor(latest["metric_name"], center)
    threshold = max(MAD_MULTIPLIER * robust_sigma, floor)
    deviation = abs(observed - center)
    if deviation <= threshold:
        return None

    score = deviation / max(robust_sigma, floor / MAD_MULTIPLIER)
    return {
        "observed": observed, "median": center, "mad": mad,
        "score": score, "severity": "critical" if score >= 6 else "warning",
        "row": latest, "baseline_count": len(baseline),
    }

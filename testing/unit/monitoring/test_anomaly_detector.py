import math
import unittest
from datetime import datetime, timedelta, timezone

from monitoring.alerts.anomaly_rules import evaluate_series, metric_floor


class AnomalyDetectorTests(unittest.TestCase):
    def make_rows(self, values):
        start = datetime(2026, 11, 1, tzinfo=timezone.utc)
        return [{
            "window_start": start + timedelta(hours=i),
            "average_value": value, "metric_name": "pm25", "unit": "µg/m³",
            "source_key": 1, "source_name": "esp32_pm_sensor",
            "governorate_key": 1, "governorate_name": "Cairo", "metric_key": 1,
        } for i, value in enumerate(values)]

    def test_insufficient_baseline_is_ignored(self):
        self.assertIsNone(evaluate_series(self.make_rows([10] * 12)))

    def test_stable_series_spike_is_detected(self):
        candidate = evaluate_series(self.make_rows([10] * 15 + [80]))
        self.assertIsNotNone(candidate)
        self.assertEqual(candidate["severity"], "critical")
        self.assertEqual(candidate["observed"], 80)

    def test_normal_variation_is_not_detected(self):
        values = [10, 11, 9, 10, 12, 8, 10, 11, 9, 10, 12, 8, 10, 11, 9, 11]
        self.assertIsNone(evaluate_series(self.make_rows(values)))

    def test_non_finite_latest_value_is_ignored(self):
        self.assertIsNone(evaluate_series(self.make_rows([10] * 15 + [math.inf])))

    def test_pm_floor_is_metric_specific(self):
        self.assertEqual(metric_floor("pm25", 10), 5.0)

    def test_uv_index_is_outside_pollutant_alert_scope(self):
        rows = self.make_rows([0.0] * 15 + [1.2])
        rows[-1]["metric_name"] = "uv_index"
        self.assertIsNone(evaluate_series(rows))

    def test_weather_covariates_are_outside_pollutant_alert_scope(self):
        for metric in ("temperature_2m", "relative_humidity_2m", "precipitation"):
            rows = self.make_rows([10.0] * 15 + [80.0])
            rows[-1]["metric_name"] = metric
            with self.subTest(metric=metric):
                self.assertIsNone(evaluate_series(rows))

    def test_near_zero_ozone_baseline_uses_absolute_floor(self):
        self.assertEqual(metric_floor("ozone", 0.0), 10.0)
        rows = self.make_rows([0.0] * 15 + [1.2])
        rows[-1]["metric_name"] = "ozone"
        self.assertIsNone(evaluate_series(rows))

    def test_circular_wind_direction_is_not_scored_linearly(self):
        rows = self.make_rows([350, 355, 358, 359, 1, 2, 4, 5, 350, 355, 358, 359, 1, 2, 4, 180])
        rows[-1]["metric_name"] = "wind_direction_10m"
        self.assertIsNone(evaluate_series(rows))


if __name__ == "__main__":
    unittest.main()

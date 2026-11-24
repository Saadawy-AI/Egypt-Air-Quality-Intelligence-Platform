import pandas as pd
import pytest

from streamlit_app.gold_data import merge_gold_batches


def _row(average, count, minimum, maximum, latitude=30.0, longitude=31.0):
    return {
        "window_start": pd.Timestamp("2026-11-23T02:00:00"),
        "window_end": pd.Timestamp("2026-11-23T03:00:00"),
        "window_date": pd.Timestamp("2026-11-23").date(),
        "source": "openaq",
        "governorate": None,
        "metric": "pm25",
        "unit": "ug/m3",
        "average_value": average,
        "minimum_value": minimum,
        "maximum_value": maximum,
        "measurement_count": count,
        "latitude": latitude,
        "longitude": longitude,
    }


def test_merge_gold_batches_uses_weighted_average_and_extrema():
    frame = pd.DataFrame([
        _row(10.0, 2, 5.0, 15.0, 30.0, 31.0),
        _row(20.0, 1, 20.0, 20.0, 32.0, 33.0),
    ])

    result = merge_gold_batches(frame)

    assert len(result) == 1
    row = result.iloc[0]
    assert row["average_value"] == pytest.approx(40.0 / 3.0)
    assert row["measurement_count"] == 3
    assert row["minimum_value"] == 5.0
    assert row["maximum_value"] == 20.0
    assert row["latitude"] == pytest.approx(92.0 / 3.0)
    assert row["longitude"] == pytest.approx(95.0 / 3.0)
    assert pd.isna(row["governorate"])


def test_merge_gold_batches_keeps_groups_with_different_metrics_separate():
    first = _row(10.0, 2, 5.0, 15.0)
    second = _row(50.0, 1, 50.0, 50.0)
    second["metric"] = "pm10"

    result = merge_gold_batches(pd.DataFrame([first, second]))

    assert len(result) == 2
    assert set(result["metric"]) == {"pm25", "pm10"}


def test_merge_gold_batches_handles_empty_input():
    result = merge_gold_batches(pd.DataFrame())

    assert result.empty
    assert "average_value" in result.columns

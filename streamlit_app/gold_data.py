import pandas as pd


GROUP_COLUMNS = [
    "window_start",
    "window_end",
    "window_date",
    "source",
    "governorate",
    "metric",
    "unit",
]

OUTPUT_COLUMNS = GROUP_COLUMNS + [
    "average_value",
    "minimum_value",
    "maximum_value",
    "measurement_count",
    "latitude",
    "longitude",
]


def merge_gold_batches(frame: pd.DataFrame) -> pd.DataFrame:
    """Merge per-batch partial aggregates into canonical hourly aggregates."""
    if frame.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    frame = frame.copy()
    frame["measurement_count"] = pd.to_numeric(
        frame["measurement_count"], errors="coerce"
    ).fillna(0)
    weights = frame["measurement_count"]
    frame["_weighted_value"] = frame["average_value"] * weights
    frame["_weighted_latitude"] = frame["latitude"] * weights
    frame["_weighted_longitude"] = frame["longitude"] * weights
    frame["_latitude_count"] = weights.where(frame["latitude"].notna(), 0)
    frame["_longitude_count"] = weights.where(frame["longitude"].notna(), 0)

    merged = (
        frame.groupby(GROUP_COLUMNS, dropna=False, as_index=False)
        .agg(
            _weighted_value=("_weighted_value", "sum"),
            measurement_count=("measurement_count", "sum"),
            minimum_value=("minimum_value", "min"),
            maximum_value=("maximum_value", "max"),
            _weighted_latitude=("_weighted_latitude", "sum"),
            _weighted_longitude=("_weighted_longitude", "sum"),
            _latitude_count=("_latitude_count", "sum"),
            _longitude_count=("_longitude_count", "sum"),
        )
    )
    merged["average_value"] = (
        merged["_weighted_value"] / merged["measurement_count"]
    )
    merged["latitude"] = (
        merged["_weighted_latitude"]
        / merged["_latitude_count"].replace(0, float("nan"))
    )
    merged["longitude"] = (
        merged["_weighted_longitude"]
        / merged["_longitude_count"].replace(0, float("nan"))
    )
    return merged[OUTPUT_COLUMNS]

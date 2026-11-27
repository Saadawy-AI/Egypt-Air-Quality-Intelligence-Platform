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
    "latitude_count",
    "longitude_count",
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

    # Coordinate averages have their own denominators in Gold. Older batches
    # may not carry them, so retain a null-aware compatibility fallback.
    if "latitude_count" in frame:
        latitude_counts = pd.to_numeric(frame["latitude_count"], errors="coerce").fillna(0)
    else:
        latitude_counts = weights.where(frame["latitude"].notna(), 0)
    if "longitude_count" in frame:
        longitude_counts = pd.to_numeric(frame["longitude_count"], errors="coerce").fillna(0)
    else:
        longitude_counts = weights.where(frame["longitude"].notna(), 0)

    frame["_latitude_count"] = latitude_counts
    frame["_longitude_count"] = longitude_counts
    frame["_weighted_latitude"] = frame["latitude"].fillna(0) * latitude_counts
    frame["_weighted_longitude"] = frame["longitude"].fillna(0) * longitude_counts

    merged = (
        frame.groupby(GROUP_COLUMNS, dropna=False, as_index=False)
        .agg(
            _weighted_value=("_weighted_value", "sum"),
            measurement_count=("measurement_count", "sum"),
            minimum_value=("minimum_value", "min"),
            maximum_value=("maximum_value", "max"),
            _weighted_latitude=("_weighted_latitude", "sum"),
            _weighted_longitude=("_weighted_longitude", "sum"),
            latitude_count=("_latitude_count", "sum"),
            longitude_count=("_longitude_count", "sum"),
        )
    )
    merged["average_value"] = (
        merged["_weighted_value"] / merged["measurement_count"].replace(0, float("nan"))
    )
    merged["latitude"] = (
        merged["_weighted_latitude"] / merged["latitude_count"].replace(0, float("nan"))
    )
    merged["longitude"] = (
        merged["_weighted_longitude"] / merged["longitude_count"].replace(0, float("nan"))
    )
    return merged[OUTPUT_COLUMNS]

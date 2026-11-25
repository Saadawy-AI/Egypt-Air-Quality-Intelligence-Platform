"""Read-only Bronze/Silver OpenAQ lineage and coverage audit."""
import json
from pathlib import Path

import pyarrow.dataset as ds


ROOT = Path("/opt/project-data")


def summarize(path):
    files = list(path.rglob("*.parquet"))
    if not files:
        return {"path": str(path), "files": 0, "rows": 0}
    dataset = ds.dataset(str(path), format="parquet")
    available = set(dataset.schema.names)
    columns = [
        name for name in (
            "record_id", "source", "event_time", "timestamp",
            "governorate", "topic", "partition", "offset",
            "kafka_key", "raw_value", "kafka_timestamp"
        ) if name in available
    ]
    table = dataset.to_table(columns=columns)
    values = table.to_pydict()
    result = {
        "path": str(path),
        "files": len(files),
        "rows": table.num_rows,
        "columns": columns,
    }
    if "kafka_key" in values:
        keys = [value for value in values["kafka_key"] if value is not None]
        result["distinct_kafka_keys"] = len(set(keys))
        result["duplicate_kafka_keys"] = len(keys) - len(set(keys))
        offsets = [value for value in values.get("offset", []) if value is not None]
        result["kafka_offsets"] = {
            "min": min(offsets) if offsets else None,
            "max": max(offsets) if offsets else None,
        }
    if "record_id" in values:
        ids = [value for value in values["record_id"] if value is not None]
        result["distinct_record_ids"] = len(set(ids))
        result["duplicate_record_ids"] = len(ids) - len(set(ids))
    if "source" in values:
        result["sources"] = sorted({value for value in values["source"] if value})
    if "governorate" in values:
        result["governorates"] = sorted({
            value for value in values["governorate"] if value
        })
    time_column = "event_time" if "event_time" in values else "timestamp"
    if time_column in values:
        timestamps = [value for value in values[time_column] if value is not None]
        result["first_event_time"] = str(min(timestamps)) if timestamps else None
        result["last_event_time"] = str(max(timestamps)) if timestamps else None
    return result


def main():
    silver_root = ROOT / "silver" / "openaq_measurements"
    summaries = [
        summarize(ROOT / "bronze" / "openaq_measurements" / "ingestion_date=2026-11-23"),
        summarize(silver_root / "ingestion_date=2026-11-23"),
        summarize(silver_root / "batch_id=historical_backfill_v1"),
    ]
    batch_summaries = []
    for batch_id in range(7, 19):
        path = silver_root / f"batch_id={batch_id:020d}"
        if path.is_dir():
            batch_summaries.append(summarize(path))
    print(json.dumps({
        "daily_and_historical": summaries,
        "streaming_silver_batches": batch_summaries,
    }, indent=2, default=str))


if __name__ == "__main__":
    main()

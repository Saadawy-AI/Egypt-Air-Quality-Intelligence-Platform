"""Load an exported OpenAQ PM2.5 history into the Silver lake.

The loader is dry-run by default. It writes one immutable, uniquely named
Silver directory only when --write is supplied. Existing record IDs are
excluded before writing so historical data cannot inflate existing Gold
aggregates on replay.
"""
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq


SILVER_SCHEMA = pa.schema([
    pa.field("record_id", pa.string()),
    pa.field("schema_version", pa.string()),
    pa.field("source", pa.string()),
    pa.field("country_code", pa.string()),
    pa.field("governorate", pa.string()),
    pa.field("governorate_ar", pa.string()),
    pa.field("latitude", pa.float64()),
    pa.field("longitude", pa.float64()),
    pa.field("timestamp", pa.string()),
    pa.field("event_time", pa.timestamp("us")),
    pa.field("data", pa.map_(pa.string(), pa.string())),
    pa.field("metadata", pa.map_(pa.string(), pa.string())),
    pa.field("topic", pa.string()),
    pa.field("partition", pa.int32()),
    pa.field("offset", pa.int64()),
    pa.field("kafka_key", pa.string()),
    pa.field("kafka_timestamp", pa.timestamp("us")),
    pa.field("ingestion_timestamp", pa.timestamp("us")),
    pa.field("ingestion_date", pa.date32()),
    pa.field("batch_id", pa.string()),
])


def utc_naive(value):
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).replace(tzinfo=None)


def read_jsonl(path):
    records = []
    with Path(path).open("r", encoding="utf-8") as source:
        for line_number, line in enumerate(source, start=1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
                if not record.get("record_id"):
                    metadata = record.get("metadata") or {}
                    data = record.get("data") or {}
                    sensor_id = metadata.get("sensor_id")
                    pollutant = data.get("pollutant")
                    timestamp = record.get("timestamp")
                    if not all((sensor_id, pollutant, timestamp)):
                        raise ValueError(
                            f"Historical record at line {line_number} cannot form a stable record_id."
                        )
                    record["record_id"] = (
                        f"{record.get('source')}:{sensor_id}:{pollutant}:{timestamp}"
                    )
                records.append(record)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"Invalid JSONL record at line {line_number}."
                ) from exc
    return records


def deduplicate_records(records, existing_ids):
    unique = {}
    for record in records:
        record_id = record.get("record_id")
        if not record_id:
            raise ValueError("Historical record is missing record_id.")
        current = unique.get(record_id)
        if current is not None:
            if (
                current.get("timestamp") != record.get("timestamp")
                or current.get("data") != record.get("data")
            ):
                raise ValueError(
                    f"Conflicting historical values for record_id={record_id}."
                )
            continue
        unique[record_id] = record

    return [
        record for record_id, record in unique.items()
        if record_id not in existing_ids
    ]


def existing_record_ids(silver_root):
    root = Path(silver_root)
    if not root.is_dir() or not any(root.rglob("*.parquet")):
        return set()
    table = ds.dataset(
        str(root),
        format="parquet",
        partitioning="hive",
        exclude_invalid_files=True,
    ).to_table(columns=["record_id"])
    return set(value for value in table.column("record_id").to_pylist() if value)


def build_table(records, batch_id):
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = []
    for record in records:
        rows.append({
            "record_id": record["record_id"],
            "schema_version": record.get("schema_version"),
            "source": record.get("source"),
            "country_code": record.get("country_code"),
            "governorate": record.get("governorate"),
            "governorate_ar": record.get("governorate_ar"),
            "latitude": record.get("latitude"),
            "longitude": record.get("longitude"),
            "timestamp": record["timestamp"],
            "event_time": utc_naive(record["timestamp"]),
            "data": [
                (str(key), None if value is None else str(value))
                for key, value in (record.get("data") or {}).items()
            ],
            "metadata": [
                (str(key), None if value is None else str(value))
                for key, value in (record.get("metadata") or {}).items()
            ],
            "topic": "openaq-measurements",
            "partition": None,
            "offset": None,
            "kafka_key": record["record_id"],
            "kafka_timestamp": None,
            "ingestion_timestamp": now,
            "ingestion_date": now.date(),
            "batch_id": batch_id,
        })
    return pa.Table.from_pylist(rows, schema=SILVER_SCHEMA)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument(
        "--silver-root",
        type=Path,
        default=Path("/opt/project-data/silver/openaq_measurements"),
    )
    parser.add_argument(
        "--batch-id",
        default="historical_backfill_v1",
        help="Unique Silver subdirectory identifier; never reuse an existing ID.",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Write the new immutable Silver batch. Default is a dry-run.",
    )
    args = parser.parse_args()

    if not args.batch_id or "/" in args.batch_id or "\\\\" in args.batch_id:
        parser.error("--batch-id must be a non-empty directory-safe identifier.")

    output_dir = args.silver_root / f"batch_id={args.batch_id}"
    if args.write and output_dir.exists():
        parser.error(f"Refusing to overwrite existing Silver batch: {output_dir}")

    records = read_jsonl(args.input)
    existing_ids = existing_record_ids(args.silver_root)
    new_records = deduplicate_records(records, existing_ids)
    table = build_table(new_records, args.batch_id)

    print({
        "input_records": len(records),
        "existing_record_ids": len(existing_ids),
        "new_unique_records": table.num_rows,
        "batch_id": args.batch_id,
        "write_enabled": args.write,
        "output_dir": str(output_dir),
    })

    if not args.write or table.num_rows == 0:
        return

    output_dir.mkdir(parents=True, exist_ok=False)
    pq.write_table(
        table,
        output_dir / "part-00000.parquet",
        compression="snappy",
    )
    print({"written_rows": table.num_rows, "output": str(output_dir)})


if __name__ == "__main__":
    main()

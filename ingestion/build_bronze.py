import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds

from ingestion.open_meteo_air_quality.client import (
    OpenMeteoAirQualityClient,
)
from ingestion.open_meteo_weather.client import (
    OpenMeteoWeatherClient,
)
from ingestion.openaq.client import OpenAQClient


TOPICS = {
    "open_meteo_air_quality": "air_quality",
    "open_meteo_weather": "weather",
    "openaq": "openaq_measurements",
}

BRONZE_SCHEMA = pa.schema([
    pa.field("kafka_key", pa.string()),
    pa.field("raw_value", pa.string()),
    pa.field("topic", pa.string()),
    pa.field("partition", pa.int32()),
    pa.field("offset", pa.int64()),
    pa.field("kafka_timestamp", pa.timestamp("us", tz="UTC")),
    pa.field("ingestion_timestamp", pa.timestamp("us", tz="UTC")),
    pa.field("ingestion_date", pa.string()),
])


def _stable_json(value):
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _bronze_row(source, topic, payload, offset, ingested_at):
    location = payload.get("governorate") or payload.get("location_name")
    timestamp = payload.get("timestamp") or payload.get("datetime")
    kafka_key = (
        f"{source}:{location or 'unknown'}:"
        f"{timestamp or ingested_at.isoformat()}"
    )
    return {
        "kafka_key": kafka_key,
        "raw_value": _stable_json(payload),
        "topic": topic,
        "partition": offset % 3,
        "offset": offset,
        "kafka_timestamp": ingested_at,
        "ingestion_timestamp": ingested_at,
        "ingestion_date": ingested_at.date().isoformat(),
    }


def fetch_meteo_raw_data():
    rows = []
    next_offset = 0
    ingested_at = datetime.now(timezone.utc)

    for client_type, source in (
        (OpenMeteoAirQualityClient, "open_meteo_air_quality"),
        (OpenMeteoWeatherClient, "open_meteo_weather"),
    ):
        client = client_type()
        try:
            source_records = client.get_all_governorates()
            topic = TOPICS[source]
            for payload in source_records:
                payload_with_source = dict(payload)
                payload_with_source["source"] = source
                rows.append(
                    _bronze_row(
                        source,
                        topic,
                        payload_with_source,
                        next_offset,
                        ingested_at,
                    )
                )
                next_offset += 1
        finally:
            client.close()

    return rows


def fetch_openaq_raw_data(max_locations=None):
    rows = []
    ingested_at = datetime.now(timezone.utc)
    offset = 0

    with OpenAQClient() as client:
        locations = client.get_all_egypt_locations()
        if max_locations is not None:
            locations = locations[:max_locations]

        for location in locations:
            location_id = location.get("id")
            if location_id is None:
                continue

            sensors = client.get_sensors(location_id)
            sensor_by_id = {
                sensor.get("id"): sensor
                for sensor in sensors
                if sensor.get("id") is not None
            }
            latest = client.get_latest_measurements(location_id)

            for measurement in latest:
                sensor_id = measurement.get("sensorsId")
                sensor = sensor_by_id.get(sensor_id)
                payload = {
                    "source": "openaq",
                    "location": location,
                    "sensor": sensor,
                    "measurement": measurement,
                }
                rows.append(
                    _bronze_row(
                        "openaq",
                        TOPICS["openaq"],
                        payload,
                        offset,
                        ingested_at,
                    )
                )
                offset += 1

    return rows


def write_bronze(rows: Iterable[dict], bronze_base: Path):
    rows = list(rows)
    if not rows:
        raise RuntimeError("No raw records were fetched.")

    existing_raw_hashes = set()
    if bronze_base.exists():
        for topic in TOPICS.values():
            topic_dir = bronze_base / topic
            if not topic_dir.exists() or not any(topic_dir.rglob("*.parquet")):
                continue

            try:
                existing = ds.dataset(
                    topic_dir,
                    format="parquet",
                    partitioning="hive",
                )
                existing_table = existing.to_table(
                    columns=["raw_value"]
                )
            except Exception as error:
                raise RuntimeError(
                    f"Unable to read existing Bronze records from {topic_dir}"
                ) from error

            existing_raw_hashes.update(
                hashlib.sha256(raw_value.encode("utf-8")).hexdigest()
                for raw_value in existing_table["raw_value"].to_pylist()
                if isinstance(raw_value, str)
            )

    unique_rows = []
    seen_raw_hashes = set(existing_raw_hashes)
    for row in rows:
        raw_value = row["raw_value"]
        raw_hash = hashlib.sha256(
            raw_value.encode("utf-8")
        ).hexdigest()
        if raw_hash in seen_raw_hashes:
            continue
        unique_rows.append(row)
        seen_raw_hashes.add(raw_hash)

    if not unique_rows:
        print(
            json.dumps(
                {
                    "bronze_base": str(bronze_base),
                    "rows_written": 0,
                    "status": "no_new_raw_records",
                },
                ensure_ascii=False,
            )
        )
        return 0

    rows = unique_rows

    table = pa.Table.from_pylist(rows, schema=BRONZE_SCHEMA)
    ingestion_dates = sorted(set(table["ingestion_date"].to_pylist()))

    written = 0
    for ingestion_date in ingestion_dates:
        partition_rows = table.filter(
            pa.compute.equal(table["ingestion_date"], ingestion_date)
        )

        partition_topics = sorted(set(partition_rows["topic"].to_pylist()))
        for topic in partition_topics:
            topic_table = partition_rows.filter(
                pa.compute.equal(partition_rows["topic"], topic)
            )
            topic_table = topic_table.select([
                "kafka_key",
                "raw_value",
                "topic",
                "partition",
                "offset",
                "kafka_timestamp",
                "ingestion_timestamp",
                "ingestion_date",
            ])
            dataset_dir = bronze_base / topic / f"ingestion_date={ingestion_date}"
            dataset_dir.mkdir(parents=True, exist_ok=True)
            output_file = dataset_dir / (
                f"raw-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')}.parquet"
            )
            pq.write_table(
                topic_table,
                output_file,
                compression="snappy",
            )
            written += topic_table.num_rows

    return written


def parse_args():
    parser = argparse.ArgumentParser(
        description="Fetch raw data and write it to the Bronze Parquet layer."
    )
    parser.add_argument(
        "--bronze-base",
        default=os.getenv(
            "BRONZE_BASE_PATH",
            str(Path(__file__).resolve().parents[1] / "data" / "bronze"),
        ),
    )
    parser.add_argument(
        "--max-openaq-locations",
        type=int,
        default=None,
        help="Optional limit for smoke-testing OpenAQ.",
    )
    parser.add_argument(
        "--skip-openaq",
        action="store_true",
        help="Fetch only the two Open-Meteo sources.",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    rows = fetch_meteo_raw_data()
    if not args.skip_openaq:
        rows.extend(
            fetch_openaq_raw_data(
                max_locations=args.max_openaq_locations,
            )
        )

    written = write_bronze(
        rows,
        Path(args.bronze_base),
    )
    print(
        json.dumps(
            {
                "bronze_base": str(args.bronze_base),
                "rows_written": written,
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()

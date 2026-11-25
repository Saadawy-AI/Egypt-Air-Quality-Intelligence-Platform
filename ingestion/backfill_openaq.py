"""One-off OpenAQ PM2.5 historical backfill.

Dry-run by default. Use --export-jsonl for a historical offline load, or
--publish with --recent-days for watermark-safe recent streaming records.
"""
import argparse
import json
from pathlib import Path
from datetime import datetime, timedelta, timezone

from ingestion.config.governorates import GOVERNORATES
from ingestion.processors.normalizer import normalize_openaq_measurement
from ingestion.processors.validator import get_validation_errors
from ingestion.producers.kafka_producer import KafkaProducerClient
from ingestion.openaq.publisher import OpenAQPublisher
from ingestion.openaq.client import OpenAQClient
from ingestion.utils.logger import get_logger


logger = get_logger("OpenAQHistoricalBackfill")
YEAR_WINDOW = timedelta(days=365)
TARGET_PARAMETER = "pm25"


def _parse_utc(value):
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _format_utc(value):
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _governorate_for_location(location):
    candidates = (
        location.get("name"),
        location.get("locality"),
    )
    for candidate in candidates:
        if candidate in GOVERNORATES:
            return candidate
    return None


def collect_historical_pm25(client, recent_days=None):
    locations = client.get_all_egypt_locations()
    records = []
    skipped_sensors = 0
    sensors_seen = 0

    for location in locations:
        location_id = location.get("id")
        if location_id is None:
            continue

        governorate = _governorate_for_location(location)
        sensors = client.get_sensors(location_id)

        for sensor in sensors:
            parameter = (sensor.get("parameter") or {}).get("name")
            if (parameter or "").strip().lower() != TARGET_PARAMETER:
                continue

            sensors_seen += 1
            first = (sensor.get("datetimeFirst") or {}).get("utc")
            last = (sensor.get("datetimeLast") or {}).get("utc")
            if not first or not last:
                skipped_sensors += 1
                logger.warning(
                    "Skipping PM2.5 sensor without complete history metadata | sensor_id=%s",
                    sensor.get("id"),
                )
                continue

            start = _parse_utc(first)
            end = _parse_utc(last)
            if recent_days is not None:
                if end < datetime.now(timezone.utc) - timedelta(days=recent_days):
                    logger.info(
                        "Skipping inactive PM2.5 sensor outside recent window | sensor_id=%s",
                        sensor.get("id"),
                    )
                    continue
                start = max(start, end - timedelta(days=recent_days))
            if end < start:
                skipped_sensors += 1
                continue

            cursor = start
            sensor_records = 0
            while cursor <= end:
                window_end = min(cursor + YEAR_WINDOW, end)
                measurements = client.get_hourly_measurements(
                    sensor.get("id"),
                    _format_utc(cursor),
                    _format_utc(window_end),
                )

                for measurement in measurements:
                    record = normalize_openaq_measurement(
                        measurement=measurement,
                        location=location,
                        sensor=sensor,
                        governorate=governorate,
                    )
                    errors = get_validation_errors(record)
                    if errors:
                        logger.warning(
                            "Skipping invalid historical PM2.5 record | sensor_id=%s errors=%s",
                            sensor.get("id"),
                            errors,
                        )
                        continue

                    record["metadata"]["ingestion_mode"] = "historical_backfill"
                    records.append(record)
                    sensor_records += 1

                cursor = window_end + timedelta(seconds=1)

            logger.info(
                "Historical PM2.5 sensor collected | sensor_id=%s records=%s",
                sensor.get("id"),
                sensor_records,
            )

    return records, sensors_seen, skipped_sensors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    output_group = parser.add_mutually_exclusive_group()
    output_group.add_argument(
        "--publish",
        action="store_true",
        help="Publish recent records to Kafka (requires --recent-days).",
    )
    output_group.add_argument(
        "--export-jsonl",
        type=Path,
        help="Export validated normalized records to a JSONL file for offline backfill.",
    )
    parser.add_argument(
        "--recent-days",
        type=int,
        default=None,
        help="Limit collection to the most recent N days per sensor.",
    )
    args = parser.parse_args()
    if args.recent_days is not None and args.recent_days <= 0:
        parser.error("--recent-days must be a positive integer")
    if args.publish and args.recent_days is None:
        parser.error(
            "--publish requires --recent-days to protect the streaming watermark. "
            "Use --export-jsonl for historical data."
        )

    client = OpenAQClient()
    producer = None
    try:
        records, sensors_seen, skipped_sensors = collect_historical_pm25(
            client,
            recent_days=args.recent_days,
        )
        print(
            {
                "target_parameter": TARGET_PARAMETER,
                "sensors_seen": sensors_seen,
                "sensors_skipped": skipped_sensors,
                "valid_records": len(records),
                "publish_enabled": args.publish,
                "export_enabled": args.export_jsonl is not None,
            }
        )

        if args.export_jsonl is not None:
            args.export_jsonl.parent.mkdir(parents=True, exist_ok=True)
            with args.export_jsonl.open("w", encoding="utf-8") as output:
                for record in records:
                    output.write(json.dumps(record, ensure_ascii=False) + "\n")
            print({"exported": len(records), "path": str(args.export_jsonl)})
            return

        if not args.publish or not records:
            return

        producer = KafkaProducerClient()
        result = OpenAQPublisher(producer_client=producer).publish(records)
        print(
            {
                "published": result["sent"],
                "failed": result["failed"],
                "success_rate": result["success_rate"],
            }
        )
        if result["failed"]:
            raise RuntimeError(
                f"Historical backfill failed to publish {result['failed']} records."
            )
    finally:
        client.close()
        if producer is not None:
            producer.close()


if __name__ == "__main__":
    main()

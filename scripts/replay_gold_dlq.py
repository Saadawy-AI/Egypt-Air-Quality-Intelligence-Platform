"""Safely replay one validated Gold dead-letter notification to Kafka.

Run inside the Airflow container:
  python /opt/airflow/scripts/replay_gold_dlq.py --file /opt/project-data/gold/dead_letter/<name>.json
  python /opt/airflow/scripts/replay_gold_dlq.py --file /opt/project-data/gold/dead_letter/<name>.json --publish

Without --publish the command validates and prints the notification only.
"""
import argparse
import json
import os
from pathlib import Path

import pyarrow.dataset as ds
from kafka import KafkaProducer

from ingestion.config.kafka_security import kafka_python_security_config

DLQ_ROOT = Path(os.getenv("GOLD_DLQ_PATH", "/opt/project-data/gold/dead_letter")).resolve()
SILVER_ROOT = Path(os.getenv("SILVER_BASE_PATH", "/opt/project-data/silver")).resolve()
TOPIC = os.getenv("SILVER_NOTIFICATION_TOPIC", "silver-batch-notifications")


def validate_entry(path: Path):
    resolved = path.resolve()
    if resolved.parent != DLQ_ROOT or resolved.suffix.lower() != ".json":
        raise ValueError(f"DLQ file must be a direct .json child of {DLQ_ROOT}")
    payload = json.loads(resolved.read_text(encoding="utf-8"))
    event = payload.get("notification")
    if not isinstance(event, dict):
        raise ValueError("DLQ payload has no notification object")

    dataset = str(event.get("dataset") or "")
    if dataset not in {"air_quality", "openaq_measurements", "weather"}:
        raise ValueError(f"Unsupported Silver dataset: {dataset!r}")
    batch_id = int(event["silver_batch_id"])
    notified_rows = int(event["row_count"])
    if batch_id < 0 or notified_rows <= 0:
        raise ValueError("Silver batch id and row_count must be positive/non-negative")

    expected_path = (
        SILVER_ROOT / dataset / f"batch_id={batch_id:020d}"
    ).resolve()
    supplied_path = Path(str(event.get("silver_path", ""))).resolve()
    if supplied_path != expected_path:
        raise ValueError(
            f"Silver path mismatch: notification={supplied_path}, expected={expected_path}"
        )
    if not (expected_path / "_SUCCESS").is_file():
        raise RuntimeError(f"Silver batch is not committed: {expected_path}")

    actual_rows = ds.dataset(str(expected_path), format="parquet").count_rows()
    if actual_rows != notified_rows:
        raise RuntimeError(
            f"Refusing replay: notification row_count={notified_rows}, "
            f"actual Silver rows={actual_rows}. Repair/rebuild the Silver batch first."
        )
    return resolved, event, actual_rows


def publish(event):
    producer = KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092").split(","),
        **kafka_python_security_config(),
        acks="all",
        retries=5,
        max_in_flight_requests_per_connection=1,
    )
    try:
        future = producer.send(
            TOPIC,
            value=json.dumps(event, ensure_ascii=False).encode("utf-8"),
        )
        metadata = future.get(timeout=30)
        producer.flush(timeout=15)
        return metadata
    finally:
        producer.close(timeout=15)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file", required=True, help="One JSON file directly inside the Gold DLQ.")
    parser.add_argument(
        "--publish", action="store_true",
        help="Publish after validation, then archive the original DLQ file.",
    )
    args = parser.parse_args()

    source, event, actual_rows = validate_entry(Path(args.file))
    print(
        json.dumps(
            {"status": "validated", "file": source.name, "dataset": event["dataset"],
             "silver_batch_id": int(event["silver_batch_id"]), "rows": actual_rows,
             "topic": TOPIC, "publish_requested": args.publish},
            ensure_ascii=False,
        )
    )
    if not args.publish:
        return

    archive = DLQ_ROOT / "replayed"
    destination = archive / source.name
    try:
        archive.mkdir(parents=True, exist_ok=True)
        if destination.exists():
            raise FileExistsError(f"Replay archive already contains {destination.name}")
    except PermissionError:
        # Airflow commonly mounts project-data read-only. Kafka replay can
        # still proceed; the operator archives the source from the host after
        # receiving the broker acknowledgement.
        destination = None

    metadata = publish(event)
    archived_as = None
    if destination is not None:
        try:
            os.replace(source, destination)
            archived_as = str(destination)
        except OSError:
            archived_as = None
    print(
        json.dumps(
            {"status": "published", "topic": metadata.topic, "partition": metadata.partition,
             "offset": metadata.offset, "archived_as": archived_as,
             "archive_required": archived_as is None},
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()

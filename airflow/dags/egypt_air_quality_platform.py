"""Main Airflow control plane for the complete data platform."""
import json
import logging
import os
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

from ingestion.config.kafka_security import kafka_python_security_config
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from platform_common import db, send_failure_alert


DATA = Path("/opt/project-data")
GOLD = DATA / "gold" / "hourly_measurements_v4"
log = logging.getLogger(__name__)




def preflight():
    from kafka import KafkaAdminClient

    admin = KafkaAdminClient(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
        **kafka_python_security_config(),
        request_timeout_ms=10000,
        client_id="airflow-platform-preflight",
    )
    try:
        topics = set(admin.list_topics())
    finally:
        admin.close()
    required_topics = {"air-quality", "weather", "openaq-measurements", "silver-batch-notifications"}
    missing_topics = sorted(required_topics - topics)
    if missing_topics:
        raise RuntimeError("Kafka topics missing: " + ", ".join(missing_topics))

    with urllib.request.urlopen("http://spark-master:8080/json/", timeout=10) as response:
        spark = json.loads(response.read().decode("utf-8"))
    alive_workers = int(spark.get("aliveworkers", 0))
    active_apps = spark.get("activeapps", [])
    if alive_workers < 1:
        raise RuntimeError("Spark master reports no alive workers.")
    if len(active_apps) < 3:
        raise RuntimeError(f"Expected active Bronze/Silver/Gold Spark applications; found {len(active_apps)}.")

    with db() as conn:
        conn.execute("SELECT 1").fetchone()
    return {
        "kafka_topics": sorted(required_topics),
        "spark_alive_workers": alive_workers,
        "spark_active_apps": len(active_apps),
        "postgres": "healthy",
    }


def ingest_sources():
    os.umask(0o002)
    from ingestion.main import run_ingestion
    summary = run_ingestion()
    if not summary:
        raise RuntimeError("Ingestion returned an empty summary.")
    return summary


def _committed_gold_batches(after_epoch=None):
    batches = []
    if not GOLD.is_dir():
        return batches
    for path in GOLD.glob("batch_id=*"):
        try:
            batch_id = int(path.name.split("=", 1)[1])
        except (IndexError, ValueError):
            continue
        success_file = path / "_SUCCESS"
        if not success_file.is_file():
            continue
        modified = success_file.stat().st_mtime
        if after_epoch is not None and modified < after_epoch - 120:
            continue
        pending = [path]
        has_parquet = False
        while pending and not has_parquet:
            current = pending.pop()
            with os.scandir(current) as entries:
                for entry in entries:
                    if entry.is_file(follow_symlinks=False):
                        if entry.name.endswith(".parquet"):
                            has_parquet = True
                            break
                    elif entry.is_dir(follow_symlinks=False):
                        pending.append(Path(entry.path))
        if has_parquet:
            batches.append((batch_id, path, modified))
    return batches


def _fresh_parquet(stage, after_epoch):
    """Find fresh Parquet output without recursively walking the full data lake."""
    roots = {
        "bronze": [DATA / "bronze" / name for name in ("air_quality", "openaq_measurements", "weather")],
        "silver": [DATA / "silver" / name for name in ("air_quality", "openaq_measurements", "weather")],
    }[stage]
    newest = 0.0
    threshold = after_epoch - 120

    for root in roots:
        if not root.is_dir():
            continue
        try:
            with os.scandir(root) as partitions:
                for partition in partitions:
                    if not partition.is_dir(follow_symlinks=False):
                        continue
                    name = partition.name
                    if stage == "bronze":
                        if not name.startswith("ingestion_date="):
                            continue
                        candidate_dirs = [partition.path]
                    else:
                        if not name.startswith("batch_id="):
                            continue
                        try:
                            if partition.stat(follow_symlinks=False).st_mtime < threshold:
                                continue
                            if not (Path(partition.path) / "_SUCCESS").is_file():
                                continue
                        except OSError:
                            continue
                        candidate_dirs = []
                        try:
                            with os.scandir(partition.path) as date_partitions:
                                candidate_dirs.extend(
                                    item.path for item in date_partitions
                                    if item.is_dir(follow_symlinks=False)
                                    and item.name.startswith("ingestion_date=")
                                )
                        except OSError:
                            continue

                    for candidate_dir in candidate_dirs:
                        try:
                            with os.scandir(candidate_dir) as files:
                                for file in files:
                                    if not file.is_file(follow_symlinks=False):
                                        continue
                                    if not file.name.endswith(".parquet"):
                                        continue
                                    try:
                                        newest = max(
                                            newest,
                                            file.stat(follow_symlinks=False).st_mtime,
                                        )
                                    except OSError:
                                        continue
                        except OSError:
                            continue
        except OSError:
            continue

    return newest >= threshold, newest


def _spark_cluster_state():
    """Return (alive_workers, active_application_count) from the Spark master."""
    with urllib.request.urlopen("http://spark-master:8080/json/", timeout=10) as response:
        spark = json.loads(response.read().decode("utf-8"))
    return int(spark.get("aliveworkers", 0)), len(spark.get("activeapps", []))


def wait_for_stage(stage, timeout_seconds=1200, poll_seconds=15, **context):
    """Wait for fresh output; treat a fully deduplicated Silver cycle as a valid no-op."""
    if stage == "gold":
        ti = context.get("ti")
        silver_result = ti.xcom_pull(task_ids="wait_for_fresh_silver") if ti else None
        if silver_result and silver_result.get("no_new_data"):
            return {"stage": "gold", "no_new_data": True}

    dag_run = context.get("dag_run")
    run_start = getattr(dag_run, "start_date", None)
    interval_start_dt = run_start or context.get("data_interval_start")
    if interval_start_dt is None:
        interval_start_dt = datetime.now(timezone.utc)
    interval_start = interval_start_dt.timestamp()
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if stage in ("bronze", "silver"):
            ready, newest = _fresh_parquet(stage, interval_start)
            if ready:
                return {"stage": stage, "latest_file_epoch": newest}
        elif stage == "gold":
            fresh = _committed_gold_batches(interval_start)
            if fresh:
                batch_id, path, modified = max(fresh, key=lambda item: item[2])
                return {"batch_id": batch_id, "path": str(path), "modified_epoch": modified}
        else:
            raise ValueError(f"Unknown stage: {stage}")
        time.sleep(poll_seconds)
    if stage == "silver":
        # A timeout is only a valid "everything was deduplicated" no-op while the
        # streaming applications are demonstrably alive. If the cluster lost its
        # worker or any Bronze/Silver/Gold app, this is a stalled pipeline and
        # must fail loudly instead of turning the whole DAG run green.
        workers, apps = _spark_cluster_state()
        if workers < 1 or apps < 3:
            raise RuntimeError(
                "Silver produced no batch and the Spark cluster is unhealthy "
                f"(alive_workers={workers}, active_apps={apps}, expected >=1 and >=3)."
            )
        log.warning(
            "No new Silver batch was committed during this run; Spark apps are alive "
            "(workers=%s, apps=%s), so upstream records were likely deduplicated.",
            workers,
            apps,
        )
        return {"stage": "silver", "no_new_data": True, "spark_active_apps": apps}
    raise TimeoutError(f"{stage.upper()} produced no fresh data within {timeout_seconds}s.")


def validate_gold_quality(ti, **context):
    silver_result = ti.xcom_pull(task_ids="wait_for_fresh_silver")
    if silver_result and silver_result.get("no_new_data"):
        return {"no_new_data": True, "quality": "not_applicable_no_new_silver_batch"}
    gold_result = ti.xcom_pull(task_ids="wait_for_fresh_gold_batch")
    if not gold_result:
        raise RuntimeError("Gold stage did not return a committed batch.")
    batch_id = int(gold_result["batch_id"])
    batch_path = Path(gold_result["path"])
    parquet_file_count = 0
    pending_dirs = [batch_path]
    while pending_dirs:
        current_dir = pending_dirs.pop()
        try:
            with os.scandir(current_dir) as entries:
                for entry in entries:
                    if entry.is_file(follow_symlinks=False):
                        if entry.name.endswith(".parquet"):
                            parquet_file_count += 1
                    elif entry.is_dir(follow_symlinks=False):
                        pending_dirs.append(Path(entry.path))
        except OSError as exc:
            raise RuntimeError(
                f"Could not inspect committed Gold batch {batch_id} at {current_dir}: {exc}"
            ) from exc
    if parquet_file_count == 0 or not (batch_path / "_SUCCESS").is_file():
        raise RuntimeError(f"Gold batch {batch_id} is not a committed non-empty batch.")
    import pyarrow.dataset as ds
    dataset = ds.dataset(str(batch_path), format="parquet")
    rows = dataset.count_rows()
    if rows <= 0:
        raise RuntimeError(f"Gold batch {batch_id} contains zero rows.")
    required_columns = {
        "window_start", "window_end", "window_date", "source", "governorate",
        "metric", "average_value", "measurement_count",
    }
    missing_columns = required_columns - set(dataset.schema.names)
    if missing_columns:
        raise RuntimeError(
            f"Gold batch {batch_id} is missing required columns: {sorted(missing_columns)}"
        )

    allowed_sources = {
        "openaq", "open_meteo_air_quality", "open_meteo_weather",
        "esp32_pm_sensor", "iot_sensor_simulator", "wokwi_esp32",
    }
    checked_rows = 0
    for arrow_batch in dataset.to_batches(
        columns=sorted(required_columns),
        batch_size=4096,
    ):
        for row in arrow_batch.to_pylist():
            checked_rows += 1
            if row["average_value"] is None or row["measurement_count"] is None:
                raise RuntimeError(f"Gold batch {batch_id} contains a null value/count.")
            if int(row["measurement_count"]) <= 0:
                raise RuntimeError(f"Gold batch {batch_id} contains a non-positive measurement_count.")
            if row["source"] not in allowed_sources:
                raise RuntimeError(f"Gold batch {batch_id} contains an unapproved source: {row['source']!r}")
            if not row["governorate"] or row["governorate"] == "Unknown":
                raise RuntimeError(f"Gold batch {batch_id} contains an unresolved governorate.")
            if not row["metric"]:
                raise RuntimeError(f"Gold batch {batch_id} contains an empty metric.")
            if row["window_start"] is None or row["window_end"] is None:
                raise RuntimeError(f"Gold batch {batch_id} contains a null hourly window.")
            if row["window_end"] <= row["window_start"]:
                raise RuntimeError(f"Gold batch {batch_id} contains an invalid hourly window.")
    if checked_rows != rows:
        raise RuntimeError(
            f"Gold batch {batch_id} validation row-count mismatch: {checked_rows} != {rows}."
        )
    return {"batch_id": batch_id, "rows": rows, "parquet_files": parquet_file_count, "quality": "passed"}


def confirm_postgres_commit(ti, **context):
    result = ti.xcom_pull(task_ids="gold_data_quality_gate")
    if result and result.get("no_new_data"):
        return {"warehouse": "unchanged_no_new_gold_batch"}
    batch_id = int(result["batch_id"])
    # The loader polls the Gold directory (POLL_SECONDS=60), so the batch may not
    # be committed the instant Gold finishes. Poll for a bounded time instead of
    # failing on a race and relying on task retries to hide it.
    row = None
    deadline = time.monotonic() + 300
    while True:
        with db() as conn:
            row = conn.execute(
                "SELECT row_count, processed_at FROM processed_gold_batches WHERE batch_id = %s",
                (batch_id,),
            ).fetchone()
        if row or time.monotonic() >= deadline:
            break
        time.sleep(10)
    if not row:
        raise TimeoutError(f"PostgreSQL has not committed Gold batch {batch_id} within 300s.")
    if int(row[0]) != int(result["rows"]):
        raise RuntimeError(f"PostgreSQL row_count mismatch for Gold batch {batch_id}: {row[0]} != {result['rows']}.")
    return {"batch_id": batch_id, "rows": int(row[0]), "processed_at": row[1].isoformat(), "warehouse": "confirmed"}


def platform_health():
    with db() as conn:
        facts = conn.execute("SELECT count(*) FROM fact_hourly_measurements").fetchone()[0]
        batches = conn.execute("SELECT count(*) FROM processed_gold_batches").fetchone()[0]
    required = [DATA / "bronze", DATA / "silver", GOLD, DATA / "checkpoints" / "bronze",
                DATA / "checkpoints" / "silver", DATA / "checkpoints" / "gold_notifications_v5"]
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        raise RuntimeError("Required platform paths missing: " + ", ".join(missing))
    return {"postgres_fact_rows": facts, "processed_gold_batches": batches, "status": "healthy"}


with DAG(
    dag_id="egypt_air_quality_platform",
    description="Master orchestration: API ingestion, Kafka, Bronze, Silver, Gold, quality and PostgreSQL.",
    schedule="15 * * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    default_args={
        "owner": "air-quality-platform",
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
        "execution_timeout": timedelta(minutes=25),
        "on_failure_callback": send_failure_alert,
    },
    tags=["platform", "master", "data-quality", "end-to-end"],
) as dag:
    check = PythonOperator(task_id="preflight_kafka_spark_postgres", python_callable=preflight)
    ingestion = PythonOperator(task_id="ingest_sources_to_kafka", python_callable=ingest_sources, do_xcom_push=False)
    bronze = PythonOperator(task_id="wait_for_fresh_bronze", python_callable=wait_for_stage, op_kwargs={"stage": "bronze"})
    silver = PythonOperator(
        task_id="wait_for_fresh_silver",
        python_callable=wait_for_stage,
        op_kwargs={"stage": "silver", "timeout_seconds": 720},
    )
    gold = PythonOperator(task_id="wait_for_fresh_gold_batch", python_callable=wait_for_stage, op_kwargs={"stage": "gold"})
    quality = PythonOperator(task_id="gold_data_quality_gate", python_callable=validate_gold_quality)
    warehouse = PythonOperator(task_id="postgres_batch_and_rowcount_match", python_callable=confirm_postgres_commit)
    health = PythonOperator(task_id="platform_health_summary", python_callable=platform_health)
    check >> ingestion >> bronze >> silver >> gold >> quality >> warehouse >> health

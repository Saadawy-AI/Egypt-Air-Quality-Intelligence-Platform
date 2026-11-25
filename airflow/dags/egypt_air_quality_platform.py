"""Main Airflow control plane for the complete data platform."""
import json
import logging
import os
import socket
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

DATA = Path("/opt/project-data")
GOLD = DATA / "gold" / "hourly_measurements_v4"
log = logging.getLogger(__name__)


def send_failure_alert(context):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        log.warning("Airflow failure alert skipped: Telegram credentials are not configured.")
        return
    ti = context.get("task_instance")
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown"
    task_id = ti.task_id if ti else "unknown"
    run_id = context.get("run_id", "unknown")
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = json.dumps({
        "chat_id": chat_id,
        "text": f"Airflow task failed\nDAG: {dag_id}\nTask: {task_id}\nRun: {run_id}",
    }).encode()
    request = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    try:
        urllib.request.urlopen(request, timeout=10).read()
    except Exception:
        log.exception("Could not deliver Airflow failure alert.")


def db():
    return psycopg.connect(
        host=os.environ["PGHOST"],
        port=os.getenv("PGPORT", "5432"),
        dbname=os.environ["PGDATABASE"],
        user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"],
        connect_timeout=10,
        application_name="airflow-platform-orchestrator",
    )


def preflight():
    from kafka import KafkaAdminClient

    admin = KafkaAdminClient(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
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
    roots = {
        "bronze": [DATA / "bronze" / name for name in ("air_quality", "openaq_measurements", "weather")],
        "silver": [DATA / "silver" / name for name in ("air_quality", "openaq_measurements", "weather")],
    }[stage]
    newest = 0.0
    for root in roots:
        if root.is_dir():
            for file in root.rglob("*.parquet"):
                try:
                    newest = max(newest, file.stat().st_mtime)
                except OSError:
                    continue
    return newest >= after_epoch, newest


def wait_for_stage(stage, timeout_seconds=1200, poll_seconds=15, **context):
    """Wait for a fresh committed output produced after this DAG interval began."""
    interval_start_dt = context.get("data_interval_start") or (datetime.now(timezone.utc) - timedelta(minutes=30))
    interval_start = interval_start_dt.timestamp()
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if stage in ("bronze", "silver"):
            ready, newest = _fresh_parquet(stage, interval_start - 120)
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
    raise TimeoutError(f"{stage.upper()} produced no fresh data within {timeout_seconds}s.")


def validate_gold_quality(ti, **context):
    gold_result = ti.xcom_pull(task_ids="wait_for_fresh_gold_batch")
    if not gold_result:
        raise RuntimeError("Gold stage did not return a committed batch.")
    batch_id = int(gold_result["batch_id"])
    batch_path = Path(gold_result["path"])
    files = list(batch_path.rglob("*.parquet"))
    if not files or not (batch_path / "_SUCCESS").is_file():
        raise RuntimeError(f"Gold batch {batch_id} is not a committed non-empty batch.")
    import pyarrow.dataset as ds
    dataset = ds.dataset(str(batch_path), format="parquet")
    rows = dataset.count_rows()
    if rows <= 0:
        raise RuntimeError(f"Gold batch {batch_id} contains zero rows.")
    table = dataset.to_table(columns=["average_value", "measurement_count"])
    values = table.column("average_value").to_pylist()
    counts = table.column("measurement_count").to_pylist()
    if any(value is None for value in values) or any(count is None or int(count) <= 0 for count in counts):
        raise RuntimeError(f"Gold batch {batch_id} failed null/count quality checks.")
    return {"batch_id": batch_id, "rows": rows, "parquet_files": len(files), "quality": "passed"}


def confirm_postgres_commit(ti, **context):
    result = ti.xcom_pull(task_ids="gold_data_quality_gate")
    batch_id = int(result["batch_id"])
    with db() as conn:
        row = conn.execute(
            "SELECT row_count, processed_at FROM processed_gold_batches WHERE batch_id = %s",
            (batch_id,),
        ).fetchone()
    if not row:
        raise TimeoutError(f"PostgreSQL has not committed Gold batch {batch_id}.")
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
    silver = PythonOperator(task_id="wait_for_fresh_silver", python_callable=wait_for_stage, op_kwargs={"stage": "silver"})
    gold = PythonOperator(task_id="wait_for_fresh_gold_batch", python_callable=wait_for_stage, op_kwargs={"stage": "gold"})
    quality = PythonOperator(task_id="gold_data_quality_gate", python_callable=validate_gold_quality)
    warehouse = PythonOperator(task_id="postgres_batch_and_rowcount_match", python_callable=confirm_postgres_commit)
    health = PythonOperator(task_id="platform_health_summary", python_callable=platform_health)
    check >> ingestion >> bronze >> silver >> gold >> quality >> warehouse >> health

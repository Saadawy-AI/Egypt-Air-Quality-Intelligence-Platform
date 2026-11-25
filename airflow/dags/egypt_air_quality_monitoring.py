"""Five-minute operational monitoring for Kafka, Spark and PostgreSQL."""
import json
import os
import urllib.request
from datetime import datetime, timedelta, timezone

import psycopg
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


def send_failure_alert(context):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        print("Airflow alert skipped: Telegram credentials are not configured.")
        return
    ti = context.get("task_instance")
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown"
    task_id = ti.task_id if ti else "unknown"
    payload = json.dumps({
        "chat_id": chat_id,
        "text": f"Airflow task failed\\nDAG: {dag_id}\\nTask: {task_id}\\nRun: {context.get('run_id', 'unknown')}",
    }).encode()
    request = urllib.request.Request(
        f"https://api.telegram.org/bot{token}/sendMessage",
        data=payload, headers={"Content-Type": "application/json"},
    )
    try:
        urllib.request.urlopen(request, timeout=10).read()
    except Exception as exc:
        print(f"Could not deliver Airflow failure alert: {exc}")


def db():
    return psycopg.connect(
        host=os.environ["PGHOST"], port=os.getenv("PGPORT", "5432"),
        dbname=os.environ["PGDATABASE"], user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"], connect_timeout=8,
    )


def check_kafka():
    from kafka import KafkaAdminClient
    admin = KafkaAdminClient(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
        request_timeout_ms=8000,
        client_id="airflow-platform-monitor",
    )
    try:
        topics = set(admin.list_topics())
    finally:
        admin.close()
    required = {"air-quality", "weather", "openaq-measurements", "silver-batch-notifications"}
    missing = sorted(required - topics)
    if missing:
        raise RuntimeError("Kafka missing required topics: " + ", ".join(missing))
    return {"kafka": "healthy", "topics": len(topics)}


def check_spark():
    with urllib.request.urlopen("http://spark-master:8080/json/", timeout=8) as response:
        payload = json.loads(response.read().decode("utf-8"))
    workers = int(payload.get("aliveworkers", 0))
    active_apps = len(payload.get("activeapps", []))
    if workers < 1:
        raise RuntimeError("Spark has no alive workers.")
    if active_apps < 3:
        raise RuntimeError(f"Expected active Bronze/Silver/Gold Spark applications; found {active_apps}.")
    return {"spark": "healthy", "alive_workers": workers, "active_apps": active_apps}


def check_postgres_and_freshness():
    with db() as conn:
        row = conn.execute("""
            SELECT count(*), max(processed_at), coalesce(sum(row_count), 0)
            FROM processed_gold_batches
        """).fetchone()
        fact_count = conn.execute("SELECT count(*) FROM fact_hourly_measurements").fetchone()[0]
    batch_count, latest, loaded_rows = row
    if not batch_count or not fact_count:
        raise RuntimeError("PostgreSQL warehouse has no processed Gold batches or fact rows.")
    if latest is None or (datetime.now(timezone.utc) - latest.astimezone(timezone.utc)).total_seconds() > 6 * 3600:
        raise RuntimeError("PostgreSQL Gold loader has not committed a batch in the last 6 hours.")
    return {
        "postgres": "healthy", "fact_rows": int(fact_count),
        "processed_batches": int(batch_count), "latest_batch_at": latest.isoformat(),
        "loaded_batch_rows_total": int(loaded_rows),
    }


with DAG(
    dag_id="egypt_air_quality_monitoring",
    description="Continuous operational checks for Kafka topics, Spark workers and PostgreSQL Gold freshness.",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=4),
    default_args={
        "owner": "air-quality-platform", "retries": 1,
        "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(minutes=2),
        "on_failure_callback": send_failure_alert,
    },
    tags=["monitoring", "kafka", "spark", "postgres"],
) as dag:
    kafka = PythonOperator(task_id="check_kafka_topics", python_callable=check_kafka)
    spark = PythonOperator(task_id="check_spark_workers", python_callable=check_spark)
    postgres = PythonOperator(task_id="check_postgres_freshness", python_callable=check_postgres_and_freshness)
    [kafka, spark, postgres]

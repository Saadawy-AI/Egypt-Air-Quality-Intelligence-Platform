from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


def run_ingestion():
    from ingestion.main import run_ingestion as ingest

    ingest()


with DAG(
    dag_id="egypt_air_quality_ingestion",
    description="Collect Egyptian air-quality and weather observations hourly.",
    schedule="0 * * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "air-quality-platform",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["ingestion", "local"],
) as dag:
    ingest_sources = PythonOperator(
        task_id="ingest_sources",
        python_callable=run_ingestion,
        do_xcom_push=False,
    )

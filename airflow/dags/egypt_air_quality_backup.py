"""Daily PostgreSQL/model backup and verified Cloudflare R2 upload."""
import os
import subprocess
import sys
import tarfile
import json
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

BACKUPS = Path("/opt/airflow/backups")
MODEL_ROOT = Path("/opt/project-models/pm25")
CANDIDATE_ROOT = Path("/opt/model-candidates")


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


def create_local_backups():
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    target = BACKUPS / "postgres"
    target.mkdir(parents=True, exist_ok=True)
    dump_path = target / f"airquality_{stamp}.dump"
    env = os.environ.copy()
    env["PGPASSWORD"] = os.environ["PGPASSWORD"]
    subprocess.run([
        "pg_dump", "--format=custom", "--no-owner", "--no-acl",
        "--host", os.environ["PGHOST"], "--port", os.getenv("PGPORT", "5432"),
        "--username", os.environ["PGUSER"], "--dbname", os.environ["PGDATABASE"],
        "--file", str(dump_path),
    ], check=True, timeout=7200, env=env)
    if not dump_path.is_file() or dump_path.stat().st_size == 0:
        raise RuntimeError("Application pg_dump produced an empty backup.")

    airflow_dump = target / f"airflow_metadata_{stamp}.dump"
    airflow_env = env.copy()
    airflow_env["PGPASSWORD"] = os.environ["AIRFLOW_DB_PASSWORD"]
    subprocess.run([
        "pg_dump", "--format=custom", "--no-owner", "--no-acl",
        "--host", os.environ["PGHOST"], "--port", os.getenv("PGPORT", "5432"),
        "--username", "airflow", "--dbname", "airflow",
        "--file", str(airflow_dump),
    ], check=True, timeout=7200, env=airflow_env)
    if not airflow_dump.is_file() or airflow_dump.stat().st_size == 0:
        raise RuntimeError("Airflow metadata pg_dump produced an empty backup.")

    model_archive = BACKUPS / "models" / f"pm25_models_{stamp}.tar"
    model_archive.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(model_archive, "w") as archive:
        for root, prefix in ((MODEL_ROOT, "active"), (CANDIDATE_ROOT, "candidates")):
            if root.exists():
                for path in root.rglob("*"):
                    if path.is_file() and path.suffix.lower() in {".joblib", ".json"}:
                        archive.add(path, arcname=Path(prefix) / path.relative_to(root))
    if model_archive.stat().st_size == 0:
        raise RuntimeError("Model archive is empty; no model artifacts were included.")
    return {"postgres_dump": str(dump_path), "postgres_bytes": dump_path.stat().st_size,
            "airflow_metadata_dump": str(airflow_dump), "airflow_metadata_bytes": airflow_dump.stat().st_size,
            "model_archive": str(model_archive), "model_bytes": model_archive.stat().st_size}


def upload_and_verify():
    required = ("R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET")
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        raise RuntimeError("R2 backup is not configured; missing: " + ", ".join(missing))
    subprocess.run(
        [sys.executable, "/opt/airflow/scripts/r2_backup.py", "upload",
         "--prefix", "egypt-air-quality/backups", "--since-hours", "2"],
        check=True, timeout=7200,
    )


with DAG(
    dag_id="egypt_air_quality_backup",
    description="Create application/Airflow PostgreSQL dumps and PM2.5 model archive, then upload and verify SHA-256 in Cloudflare R2.",
    schedule="0 2 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=3),
    default_args={"owner": "air-quality-platform", "retries": 2,
                  "retry_delay": timedelta(minutes=10),
                  "execution_timeout": timedelta(hours=2, minutes=30),
                  "on_failure_callback": send_failure_alert},
    tags=["operations", "backup", "r2", "postgres"],
) as dag:
    local = PythonOperator(task_id="create_postgres_and_model_backups", python_callable=create_local_backups)
    remote = PythonOperator(task_id="upload_and_verify_r2", python_callable=upload_and_verify)
    local >> remote

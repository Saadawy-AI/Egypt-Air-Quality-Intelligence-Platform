"""Daily database, model and coordinated Spark lake backup with R2 verification."""
import hashlib
import json
import os
import subprocess
import sys
import tarfile
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from platform_common import send_failure_alert

BACKUPS = Path("/opt/airflow/backups")
MODEL_ROOT = Path("/opt/project-models/pm25")
CANDIDATE_ROOT = Path("/opt/model-candidates")
DATA_ROOT = Path("/opt/project-data")
CONTROL_URL = os.getenv("CONTROL_API_URL", "http://source-control-api:8088").rstrip("/")
SPARK_SOURCES = ("spark_bronze", "spark_silver", "spark_gold")



def control_spark(source, action):
    token = os.environ["CONTROL_API_TOKEN"]
    request = urllib.request.Request(
        f"{CONTROL_URL}/v1/sources/{source}/{action}",
        data=b"{}" if action == "start" else None,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=45) as response:
        result = json.loads(response.read())
    if result.get("result") != "ok":
        raise RuntimeError(f"Spark lifecycle action not confirmed: {source} {action}: {result}")
    print(f"Confirmed {source} {action}: {result.get('state')}")
    return result


def stop_spark_streams():
    # Stop downstream consumers first, so no active query mutates lake/checkpoint files.
    stopped = []
    try:
        for source in reversed(SPARK_SOURCES):
            control_spark(source, "stop")
            stopped.append(source)
    except Exception:
        # Best-effort rollback if a partial shutdown fails.
        for source in reversed(stopped):
            try:
                control_spark(source, "start")
            except Exception as exc:
                print(f"CRITICAL: unable to restart {source} after partial stop: {exc}")
        raise
    return {"stopped": stopped, "snapshot_window": "Spark Bronze/Silver/Gold stopped"}


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
        "--username", "airflow", "--dbname", "airflow", "--file", str(airflow_dump),
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
    return {"postgres_dump": str(dump_path), "airflow_metadata_dump": str(airflow_dump),
            "model_archive": str(model_archive)}


def create_streaming_lake_archive():
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    lake_dir = BACKUPS / "lake"
    lake_dir.mkdir(parents=True, exist_ok=True)
    archive_path = lake_dir / f"streaming_lake_{stamp}.tar"
    required = ("bronze", "silver", "gold", "checkpoints")
    missing = [name for name in required if not (DATA_ROOT / name).is_dir()]
    if missing:
        raise RuntimeError("Cannot snapshot lake; missing data directories: " + ", ".join(missing))

    manifest = {
        "created_at_utc": stamp,
        "consistency": "Spark Bronze/Silver/Gold containers stopped through authenticated allowlisted control API",
        "included": list(required),
    }
    with tarfile.open(archive_path, "w") as archive:
        for name in required:
            root = DATA_ROOT / name
            archive.add(root, arcname=f"data/{name}")
        info = json.dumps(manifest, indent=2).encode()
        import io
        archive.addfile(tarfile.TarInfo("BACKUP_MANIFEST.json"), io.BytesIO(info))
    if archive_path.stat().st_size <= 1024:
        raise RuntimeError("Streaming lake archive is unexpectedly small.")
    with tarfile.open(archive_path, "r") as check:
        members = check.getmembers()
        names = [member.name for member in members]
        if not all(any(name == f"data/{folder}" or name.startswith(f"data/{folder}/") for name in names)
                   for folder in required):
            raise RuntimeError("Lake archive validation failed: one or more required directories are absent.")
        if any(Path(name).is_absolute() or ".." in Path(name).parts for name in names):
            raise RuntimeError("Lake archive contains an unsafe path.")
    digest_state = hashlib.sha256()
    with archive_path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest_state.update(chunk)
    digest = digest_state.hexdigest()
    print(f"LAKE SNAPSHOT VERIFIED: {archive_path} bytes={archive_path.stat().st_size} sha256={digest}")
    return {"lake_archive": str(archive_path), "bytes": archive_path.stat().st_size, "sha256": digest}


def resume_spark_streams():
    errors = []
    # Start upstream first; downstream Spark jobs reconnect to the same checkpoint locations.
    for source in SPARK_SOURCES:
        try:
            control_spark(source, "start")
        except Exception as exc:
            errors.append(f"{source}: {exc}")
    if errors:
        raise RuntimeError("Spark restart incomplete: " + " | ".join(errors))
    return {"restarted": list(SPARK_SOURCES), "result": "all_running"}


def upload_and_verify(**context):
    # The resume task uses ALL_DONE, so its success alone must never authorize
    # an upload when the lake snapshot itself failed or was interrupted.
    dag_run = context.get("dag_run")
    lake_ti = dag_run.get_task_instance(task_id="archive_bronze_silver_gold_and_checkpoints") if dag_run else None
    if lake_ti is None or lake_ti.state != "success":
        lake_state = lake_ti.state if lake_ti else "missing"
        raise RuntimeError(
            f"Refusing R2 upload: coordinated lake snapshot did not succeed (state={lake_state})."
        )

    required = ("R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET")
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        raise RuntimeError("R2 backup is not configured; missing: " + ", ".join(missing))
    subprocess.run(
        [sys.executable, "/opt/airflow/scripts/r2_backup.py", "upload",
         "--prefix", "egypt-air-quality/backups", "--since-hours", "4"],
        check=True, timeout=7200,
    )


with DAG(
    dag_id="egypt_air_quality_backup",
    description="Database/model backup plus a coordinated Spark lake/checkpoint snapshot and verified R2 upload.",
    schedule="0 2 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=4),
    default_args={"owner": "air-quality-platform", "retries": 1,
                  "retry_delay": timedelta(minutes=10),
                  "execution_timeout": timedelta(hours=3, minutes=30),
                  "on_failure_callback": send_failure_alert},
    tags=["operations", "backup", "r2", "postgres", "spark", "lake"],
) as dag:
    databases_models = PythonOperator(
        task_id="create_postgres_and_model_backups", python_callable=create_local_backups)
    stop_streams = PythonOperator(
        task_id="stop_spark_streaming_for_consistent_snapshot", python_callable=stop_spark_streams)
    lake = PythonOperator(
        task_id="archive_bronze_silver_gold_and_checkpoints", python_callable=create_streaming_lake_archive)
    resume_streams = PythonOperator(
        task_id="always_resume_spark_streaming", python_callable=resume_spark_streams,
        trigger_rule=TriggerRule.ALL_DONE)
    remote = PythonOperator(
        task_id="upload_and_verify_all_backups_in_r2", python_callable=upload_and_verify)

    databases_models >> stop_streams >> lake >> resume_streams >> remote

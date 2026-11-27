"""Scheduled ML candidate training, temporal evaluation and registry."""
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from platform_common import db, send_failure_alert

MODEL_ROOT = Path("/opt/model-candidates")




def train_candidate(**context):
    run_slug = re.sub(r"[^A-Za-z0-9_-]", "_", context["run_id"])[:90]
    output = MODEL_ROOT / f"{context['ds_nodash']}_{run_slug}"
    output.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env["ML_OUTPUT_DIR"] = str(output)
    subprocess.run(
        [sys.executable, "-m", "ml.training.train_pm25"],
        check=True, timeout=7200, env=env,
    )
    metadata_path = output / "pm25_hgb_v2.json"
    model_path = output / "pm25_hgb_v2.joblib"
    if not metadata_path.is_file() or not model_path.is_file():
        raise RuntimeError("ML training did not produce both model and metadata artifacts.")
    return {"output_dir": str(output), "metadata_path": str(metadata_path), "model_path": str(model_path)}


def evaluate_and_register(ti):
    result = ti.xcom_pull(task_ids="train_pm25_candidate")
    metadata = json.loads(Path(result["metadata_path"]).read_text(encoding="utf-8"))
    evaluation = metadata.get("evaluation", {})
    model_metrics = evaluation.get("model", {})
    baseline_metrics = evaluation.get("naive_persistence_baseline", {})
    qualified = bool(evaluation.get("baseline_gate_passed"))
    with db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ml_model_registry (
                registry_id BIGSERIAL PRIMARY KEY,
                model_version TEXT NOT NULL,
                trained_at TIMESTAMPTZ NOT NULL,
                artifact_path TEXT NOT NULL,
                metadata JSONB NOT NULL,
                status TEXT NOT NULL CHECK (status IN ('qualified_candidate','rejected_candidate')),
                registered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                UNIQUE (model_version, trained_at)
            )
        """)
        conn.execute("""
            INSERT INTO ml_model_registry
                (model_version, trained_at, artifact_path, metadata, status)
            VALUES (%s, %s, %s, %s::jsonb, %s)
            ON CONFLICT (model_version, trained_at) DO NOTHING
        """, (
            metadata["model_version"], metadata["trained_at_utc"],
            result["model_path"], json.dumps(metadata),
            "qualified_candidate" if qualified else "rejected_candidate",
        ))
    if not qualified:
        return {
            "status": "rejected_candidate",
            "artifact_path": result["model_path"],
            "metrics": model_metrics,
            "baseline_metrics": baseline_metrics,
            "reason": "Candidate did not pass the naive persistence MAE gate; production model remains unchanged.",
        }
    return {"status": "qualified_candidate", "artifact_path": result["model_path"], "metrics": model_metrics}


with DAG(
    dag_id="egypt_air_quality_ml",
    description="Weekly PM2.5 candidate training, chronological evaluation and model registry; no automatic production deployment.",
    schedule="0 3 * * 0",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=3),
    default_args={
        "owner": "air-quality-platform", "retries": 1,
        "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(hours=2, minutes=15),
        "on_failure_callback": send_failure_alert,
    },
    tags=["machine-learning", "training", "evaluation", "model-registry"],
) as dag:
    train = PythonOperator(task_id="train_pm25_candidate", python_callable=train_candidate)
    register = PythonOperator(task_id="evaluate_and_register_candidate", python_callable=evaluate_and_register)
    train >> register

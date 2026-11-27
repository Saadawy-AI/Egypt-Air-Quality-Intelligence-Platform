"""Shared helpers for the Egypt Air Quality Airflow DAGs."""
import json
import logging
import os
from urllib.request import Request, urlopen

logger = logging.getLogger("EgyptAirQualityAirflow")


def send_failure_alert(context):
    """Send a compact Telegram alert; never mask the original task failure."""
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        logger.warning(
            "Airflow failure alert skipped: Telegram credentials are not configured."
        )
        return

    task_instance = context.get("task_instance")
    dag = context.get("dag")
    payload = {
        "chat_id": chat_id,
        "text": (
            "Airflow task failed\n"
            f"DAG: {getattr(dag, 'dag_id', 'unknown')}\n"
            f"Task: {getattr(task_instance, 'task_id', 'unknown')}\n"
            f"Run: {context.get('run_id', 'unknown')}"
        ),
    }
    request = Request(
        f"https://api.telegram.org/bot{token}/sendMessage",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    try:
        with urlopen(request, timeout=10) as response:
            response.read()
    except Exception:
        logger.exception("Could not deliver Airflow failure alert.")


def db():
    """Create a PostgreSQL connection using the shared Compose environment."""
    import psycopg

    required = ("PGHOST", "PGDATABASE", "PGUSER", "PGPASSWORD")
    missing = [key for key in required if not os.getenv(key)]
    if missing:
        raise RuntimeError(
            "Missing required PostgreSQL settings: " + ", ".join(missing)
        )
    return psycopg.connect(
        host=os.environ["PGHOST"],
        port=os.getenv("PGPORT", "5432"),
        dbname=os.environ["PGDATABASE"],
        user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"],
        connect_timeout=int(os.getenv("PG_CONNECT_TIMEOUT", "10")),
        application_name="egypt-air-quality-airflow",
    )

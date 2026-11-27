"""Five-minute operational monitoring for MQTT, Kafka, Spark and PostgreSQL."""
import json
import os
import threading
import time
import urllib.request
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from platform_common import db, send_failure_alert
import paho.mqtt.client as mqtt
from ingestion.config.kafka_security import kafka_python_security_config




def check_mqtt_ingress():
    received = threading.Event()
    state = {"connected_clients": None}

    def on_connect(client, userdata, flags, reason_code, properties):
        if getattr(reason_code, "is_failure", False):
            return
        client.subscribe("$SYS/broker/clients/connected", qos=0)

    def on_message(client, userdata, message):
        if message.topic == "$SYS/broker/clients/connected":
            try:
                state["connected_clients"] = int(message.payload.decode("utf-8").strip())
                received.set()
            except (TypeError, ValueError):
                return

    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="airflow-iot-ingress-health",
        protocol=mqtt.MQTTv311,
    )
    mqtt_username = os.getenv("MQTT_USERNAME")
    mqtt_password = os.getenv("MQTT_PASSWORD")
    if not mqtt_username or not mqtt_password:
        raise RuntimeError("MQTT credentials are not configured for the health probe.")
    client.username_pw_set(mqtt_username, mqtt_password)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(os.getenv("MQTT_HOST", "mqtt"), int(os.getenv("MQTT_PORT", "1883")), keepalive=15)
    client.loop_start()
    try:
        if not received.wait(timeout=15):
            raise TimeoutError("Mosquitto did not publish its $SYS connected-client metric.")
    finally:
        client.disconnect()
        client.loop_stop()

    # Mosquitto may deliver the retained $SYS snapshot from before this probe
    # connected. The core MQTT-to-Kafka bridge must account for at least one client.
    if state["connected_clients"] is None or state["connected_clients"] < 1:
        raise RuntimeError(
            f"MQTT broker reports {state['connected_clients']} connected clients; expected the bridge."
        )
    return {"mqtt": "healthy", "connected_clients": state["connected_clients"], "bridge_presence": "inferred"}


def check_kafka():
    from kafka import KafkaAdminClient
    admin = KafkaAdminClient(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
        **kafka_python_security_config(),
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
    required_apps = max(1, int(os.getenv("SPARK_EXPECTED_ACTIVE_APPS", "3")))
    grace_seconds = max(0, int(os.getenv("SPARK_STARTUP_GRACE_SECONDS", "60")))
    deadline = time.monotonic() + grace_seconds
    workers = 0
    active_apps = 0

    while True:
        with urllib.request.urlopen("http://spark-master:8080/json/", timeout=8) as response:
            payload = json.loads(response.read().decode("utf-8"))
        workers = int(payload.get("aliveworkers", 0))
        active_apps = len(payload.get("activeapps", []))
        if workers >= 1 and active_apps >= required_apps:
            return {"spark": "healthy", "alive_workers": workers, "active_apps": active_apps}
        if time.monotonic() >= deadline:
            if workers < 1:
                raise RuntimeError("Spark has no alive workers after the startup grace period.")
            raise RuntimeError(
                f"Expected {required_apps} active Spark applications after {grace_seconds}s; "
                f"found {active_apps}."
            )
        time.sleep(min(10, max(1, deadline - time.monotonic())))


def check_gold_dlq():
    dlq_path = os.getenv("GOLD_DLQ_PATH", "/opt/project-data/gold/dead_letter")
    if not os.path.isdir(dlq_path):
        return {"gold_dlq": "clear", "pending": 0}
    with os.scandir(dlq_path) as entries:
        pending = [
            entry.name for entry in entries
            if entry.is_file(follow_symlinks=False) and entry.name.endswith(".json")
        ]
    if pending:
        raise RuntimeError(
            f"Gold dead-letter queue contains {len(pending)} failed notifications: "
            + ", ".join(sorted(pending)[:10])
        )
    return {"gold_dlq": "clear", "pending": 0}


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
    max_active_tasks=1,  # Serialize health probes to reduce concurrent Airflow RTIF writes.
    dagrun_timeout=timedelta(minutes=4),
    default_args={
        "owner": "air-quality-platform", "retries": 1,
        "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(minutes=2),
        "on_failure_callback": send_failure_alert,
    },
    tags=["monitoring", "mqtt", "kafka", "spark", "postgres"],
) as dag:
    mqtt_ingress = PythonOperator(task_id="check_mqtt_to_kafka_bridge", python_callable=check_mqtt_ingress)
    kafka = PythonOperator(task_id="check_kafka_topics", python_callable=check_kafka)
    spark = PythonOperator(task_id="check_spark_workers", python_callable=check_spark)
    postgres = PythonOperator(task_id="check_postgres_freshness", python_callable=check_postgres_and_freshness)
    gold_dlq = PythonOperator(task_id="check_gold_dead_letter_queue", python_callable=check_gold_dlq)
    # The five checks are independent and intentionally run in parallel
    # (no dependency edges between mqtt_ingress, kafka, spark, postgres, gold_dlq).

import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import psycopg
import requests
from psycopg.conninfo import make_conninfo

from monitoring.alerts.anomaly_rules import evaluate_series


logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("AirQualityAnomalyDetector")

POLL_SECONDS = max(30, int(os.getenv("POLL_SECONDS", "120")))
LOOKBACK_DAYS = max(2, int(os.getenv("ANOMALY_LOOKBACK_DAYS", "8")))

QUERY = """
SELECT f.window_start, f.window_end, f.source_key, s.source_name,
       f.governorate_key, g.governorate_name, f.metric_key, m.metric_name,
       m.unit, f.average_value, f.minimum_value, f.maximum_value,
       f.measurement_count
FROM fact_hourly_measurements f
JOIN dim_source s ON s.source_key = f.source_key
JOIN dim_governorate g ON g.governorate_key = f.governorate_key
JOIN dim_metric m ON m.metric_key = f.metric_key
WHERE f.window_start >= %s AND f.window_end <= now()
ORDER BY f.source_key, f.governorate_key, f.metric_key, f.window_start
"""

INSERT_ALERT = """
INSERT INTO anomaly_alerts (
    window_start, source_key, governorate_key, metric_key,
    observed_value, baseline_median, baseline_mad, deviation_score,
    severity, message
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (window_start, source_key, governorate_key, metric_key)
DO NOTHING
RETURNING alert_id
"""

PENDING_ALERTS = """
SELECT alert_id, message
FROM anomaly_alerts
WHERE telegram_sent_at IS NULL AND suppressed_at IS NULL
ORDER BY created_at, alert_id
LIMIT 50
"""

MARK_SENT = """
UPDATE anomaly_alerts
SET telegram_sent_at = now(), telegram_message_id = %s
WHERE alert_id = %s AND telegram_sent_at IS NULL
"""


def connection_info():
    required = ("PGHOST", "PGDATABASE", "PGUSER", "PGPASSWORD")
    missing = [key for key in required if not os.getenv(key)]
    if missing:
        raise RuntimeError("Missing PostgreSQL settings: " + ", ".join(missing))
    return make_conninfo(
        host=os.environ["PGHOST"], port=os.getenv("PGPORT", "5432"),
        dbname=os.environ["PGDATABASE"], user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"], connect_timeout=10,
        application_name="egypt-air-quality-anomaly-detector",
    )


def format_alert(candidate):
    row = candidate["row"]
    unit = f" {row['unit']}" if row.get("unit") else ""
    stamp = row["window_start"].astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return (
        f"Egypt Air Quality — {candidate['severity'].upper()} anomaly\n"
        f"Metric: {row['metric_name']} ({row['source_name']})\n"
        f"Governorate: {row['governorate_name']}\n"
        f"Hour: {stamp}\n"
        f"Observed: {candidate['observed']:.3f}{unit}\n"
        f"Baseline median: {candidate['median']:.3f}{unit}\n"
        f"Robust deviation score: {candidate['score']:.2f}\n"
        f"Baseline points: {candidate['baseline_count']}"
    )


def detect_new_alerts(conn, now=None):
    now = now or datetime.now(timezone.utc)
    rows = conn.execute(QUERY, (now - timedelta(days=LOOKBACK_DAYS),)).fetchall()
    grouped = defaultdict(list)
    for row in rows:
        grouped[(row["source_key"], row["governorate_key"], row["metric_key"])].append(row)

    created = 0
    with conn.transaction():
        for series in grouped.values():
            candidate = evaluate_series(series)
            if candidate is None:
                continue
            row = candidate["row"]
            inserted = conn.execute(
                INSERT_ALERT,
                (row["window_start"], row["source_key"], row["governorate_key"],
                 row["metric_key"], candidate["observed"], candidate["median"],
                 candidate["mad"], candidate["score"], candidate["severity"],
                 format_alert(candidate)),
            ).fetchone()
            if inserted:
                created += 1
                logger.warning("Anomaly detected | alert_id=%s | severity=%s | metric=%s | governorate=%s",
                               inserted["alert_id"], candidate["severity"],
                               row["metric_name"], row["governorate_name"])
    return created


def send_pending_telegram(conn):
    token, chat_id = os.getenv("TELEGRAM_BOT_TOKEN"), os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        return 0

    sent = 0
    for alert in conn.execute(PENDING_ALERTS).fetchall():
        try:
            response = requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": alert["message"],
                      "disable_web_page_preview": True},
                timeout=10,
            )
            response.raise_for_status()
            body = response.json()
            if not body.get("ok"):
                raise RuntimeError("Telegram API did not confirm the message.")
            with conn.transaction():
                conn.execute(MARK_SENT, (str(body["result"]["message_id"]), alert["alert_id"]))
            sent += 1
        except Exception as exc:
            # Requests exceptions can include the request URL, which contains
            # the bot token. Log only the exception class; never leak secrets.
            logger.error(
                "Telegram delivery failed | alert_id=%s | error_type=%s",
                alert["alert_id"],
                type(exc).__name__,
            )
            break
    return sent


def run():
    conninfo = connection_info()
    while True:
        try:
            with psycopg.connect(conninfo, row_factory=psycopg.rows.dict_row) as conn:
                created = detect_new_alerts(conn)
                sent = send_pending_telegram(conn)
                logger.info("Anomaly cycle complete | new_alerts=%s | telegram_sent=%s", created, sent)
            time.sleep(POLL_SECONDS)
        except Exception:
            logger.exception("Anomaly detector cycle failed; retrying.")
            time.sleep(15)


if __name__ == "__main__":
    run()

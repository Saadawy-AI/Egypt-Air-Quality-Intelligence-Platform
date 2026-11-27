import logging
import os
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import psycopg
import pyarrow.dataset as ds
import streamlit as st
from psycopg.conninfo import make_conninfo

from gold_data import merge_gold_batches

def load_airflow_runs():
    import json
    from urllib.error import HTTPError, URLError
    from urllib.request import Request, urlopen

    api_base = os.getenv("AIRFLOW_API_URL", "").rstrip("/")
    user = os.getenv("AIRFLOW_API_USER", "")
    password = os.getenv("AIRFLOW_API_PASSWORD", "")
    if not api_base or not user or not password:
        raise RuntimeError("Airflow API credentials are not configured.")
    root = api_base.removesuffix("/api/v2")
    token_request = Request(
        root + "/auth/token",
        data=json.dumps({"username": user, "password": password}).encode(),
        headers={"Content-Type": "application/json"},
    )
    try:
        with urlopen(token_request, timeout=8) as response:
            token_payload = json.loads(response.read().decode("utf-8"))
        token = token_payload.get("access_token")
        if not token:
            raise RuntimeError("Airflow auth endpoint returned no access token.")
        rows = []
        for dag_id in (
            "egypt_air_quality_platform",
            "egypt_air_quality_monitoring",
            "egypt_air_quality_ml",
            "egypt_air_quality_backup",
        ):
            url = f"{api_base}/dags/{dag_id}/dagRuns?limit=1&order_by=-start_date"
            request = Request(url, headers={"Authorization": f"Bearer {token}"})
            with urlopen(request, timeout=8) as response:
                payload = json.loads(response.read().decode("utf-8"))
            runs = payload.get("dag_runs", [])
            if runs:
                run = runs[0]
                rows.append({
                    "DAG": dag_id,
                    "State": run.get("state", "unknown"),
                    "Run ID": run.get("dag_run_id"),
                    "Logical date": run.get("logical_date"),
                    "Started": run.get("start_date"),
                    "Ended": run.get("end_date"),
                })
            else:
                rows.append({"DAG": dag_id, "State": "no runs yet"})
        return rows
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", "replace")
        raise RuntimeError(f"Airflow API returned HTTP {exc.code}: {detail[:200]}") from exc
    except URLError as exc:
        raise RuntimeError(f"Airflow API is unavailable: {exc.reason}") from exc


@st.cache_data(ttl=30)
def cached_airflow_runs():
    return load_airflow_runs()


logger = logging.getLogger("EgyptAirQualityDashboard")

POSTGRES_QUERY = """
SELECT
    f.window_start,
    f.window_end,
    d.date_value AS window_date,
    s.source_name AS source,
    g.governorate_name AS governorate,
    m.metric_name AS metric,
    NULLIF(m.unit, '') AS unit,
    f.average_value,
    f.minimum_value,
    f.maximum_value,
    f.measurement_count,
    f.latitude,
    f.longitude
FROM fact_hourly_measurements AS f
JOIN dim_date AS d ON d.date_key = f.date_key
JOIN dim_source AS s ON s.source_key = f.source_key
JOIN dim_governorate AS g ON g.governorate_key = f.governorate_key
JOIN dim_metric AS m ON m.metric_key = f.metric_key
WHERE d.date_value >= %s
ORDER BY f.window_start
"""

GOLD_COLUMNS = [
    "window_start",
    "window_end",
    "window_date",
    "source",
    "governorate",
    "metric",
    "unit",
    "average_value",
    "minimum_value",
    "maximum_value",
    "measurement_count",
    "latitude",
    "longitude",
]


st.set_page_config(
    page_title="Analysis | Egypt Air Quality",
    layout="wide",
)
st.title("Analysis Dashboard")
st.caption("Operational air-quality analytics from OpenAQ and Open-Meteo. Use the sidebar to switch to the Machine Learning dashboard.")

# Source lifecycle controls are deliberately separate from the analytics view.
# Only optional simulator services are controllable; API ingestion stays always-on.
import json
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

CONTROL_API_URL = os.getenv("CONTROL_API_URL", "").rstrip("/")
CONTROL_API_TOKEN = os.getenv("CONTROL_API_TOKEN", "")

def control_api_request(path, method="GET"):
    if not CONTROL_API_URL or not CONTROL_API_TOKEN:
        raise RuntimeError("Source Control API is not configured.")
    request = Request(
        CONTROL_API_URL + path,
        method=method,
        headers={"Authorization": f"Bearer {CONTROL_API_TOKEN}"},
    )
    try:
        with urlopen(request, timeout=5) as response:
            return json.loads(response.read() or b"{}")
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", "replace")
        raise RuntimeError(f"Control API returned HTTP {exc.code}: {detail[:250]}") from exc
    except URLError as exc:
        raise RuntimeError(f"Control API is unavailable: {exc.reason}") from exc

# Dedicated master switch for the optional IoT runtime only.
# Core API ingestion, Kafka, Spark, Airflow and PostgreSQL are never touched.
with st.expander("IoT System Power", expanded=True):
    st.caption(
        "Start or stop the optional IoT layer independently. This affects only "
        "the Python simulator and Wokwi MQTT bridge; core ingestion and analytics stay online."
    )
    try:
        iot_sources = control_api_request("/v1/sources").get("sources", {})
        iot_ids = ("python_iot", "wokwi")
        iot_states = {key: iot_sources.get(key, {}) for key in iot_ids}
        iot_running = {
            key: bool(value.get("running"))
            for key, value in iot_states.items()
        }
        running_count = sum(iot_running.values())
        if running_count == len(iot_ids):
            st.success("IoT layer: ON — both optional services are running.")
        elif running_count == 0:
            st.info("IoT layer: OFF — both optional services are stopped.")
        else:
            st.warning("IoT layer: PARTIAL — only one optional service is running.")
        power_on, power_off = st.columns(2)
        if power_on.button(
            "Turn ON IoT",
            key="iot_master_start",
            type="primary",
            disabled=all(iot_running.values()),
            use_container_width=True,
        ):
            errors = []
            for source_id in iot_ids:
                state = iot_states[source_id]
                if state.get("state") == "not_deployed":
                    errors.append(f"{source_id}: not deployed")
                    continue
                if not iot_running[source_id]:
                    try:
                        control_api_request(f"/v1/sources/{source_id}/start", method="POST")
                    except Exception as exc:
                        errors.append(f"{source_id}: {exc}")
            if errors:
                st.error("IoT start was partial: " + " | ".join(errors))
            else:
                st.success("IoT start requested for all optional services.")
            st.rerun()
        if power_off.button(
            "Turn OFF IoT",
            key="iot_master_stop",
            disabled=not any(iot_running.values()),
            use_container_width=True,
        ):
            errors = []
            for source_id in reversed(iot_ids):
                if iot_running[source_id]:
                    try:
                        control_api_request(f"/v1/sources/{source_id}/stop", method="POST")
                    except Exception as exc:
                        errors.append(f"{source_id}: {exc}")
            if errors:
                st.error("IoT stop was partial: " + " | ".join(errors))
            else:
                st.success("IoT stop requested for all optional services.")
            st.rerun()
    except Exception as exc:
        st.warning(f"IoT master control is unavailable: {exc}")

with st.expander("IoT Sources Control", expanded=False):
    st.caption("OpenAQ and Open-Meteo sources are always on and cannot be stopped here.")
    st.dataframe(
        pd.DataFrame([
            {"Source": "OpenAQ", "Mode": "Always On", "Control": "Locked On"},
            {"Source": "Open-Meteo Air Quality", "Mode": "Always On", "Control": "Locked On"},
            {"Source": "Open-Meteo Weather", "Mode": "Always On", "Control": "Locked On"},
        ]),
        hide_index=True,
        width="stretch",
    )
    try:
        source_state = control_api_request("/v1/sources").get("sources", {})
        for source_id, label in (
            ("python_iot", "Python IoT Simulator"),
            ("wokwi", "Wokwi ESP32"),
        ):
            state = source_state.get(source_id, {})
            running = bool(state.get("running"))
            status_text = state.get("state", "unknown")
            st.write(f"**{label}** — {status_text}")
            if source_id == "wokwi" and state.get("topic_prefix"):
                st.caption("Copy this topic prefix into TOPIC_PREFIX in the Wokwi sketch:")
                st.code(state["topic_prefix"])
            if status_text == "not_deployed":
                st.info(f"{label} has not been provisioned as a controllable service yet.")
                continue
            col_start, col_stop = st.columns(2)
            if col_start.button(f"Start {label}", key=f"{source_id}_start", disabled=running):
                try:
                    control_api_request(f"/v1/sources/{source_id}/start", method="POST")
                    st.success(f"{label} start confirmed.")
                    st.rerun()
                except Exception as exc:
                    st.error(str(exc))
            if col_stop.button(f"Stop {label}", key=f"{source_id}_stop", disabled=not running):
                try:
                    control_api_request(f"/v1/sources/{source_id}/stop", method="POST")
                    st.success(f"{label} stop confirmed.")
                    st.rerun()
                except Exception as exc:
                    st.error(str(exc))
    except Exception as exc:
        st.warning(f"Optional-source controls are currently unavailable: {exc}")

with st.expander("Airflow Platform Orchestration", expanded=True):
    st.caption("Latest run status from the Airflow control plane. Scheduled ingestion is owned by the master platform DAG.")
    try:
        airflow_rows = cached_airflow_runs()
        airflow_frame = pd.DataFrame(airflow_rows)
        st.dataframe(airflow_frame, hide_index=True, width="stretch")
        states = [str(row.get("State", "unknown")).lower() for row in airflow_rows]
        if any(state == "failed" for state in states):
            st.error("At least one Airflow workflow has a failed latest run. Open Airflow for task-level logs.")
        elif any(state in ("running", "queued") for state in states):
            st.info("Airflow workflows are currently running or queued.")
        elif all(state == "success" for state in states if state != "no runs yet"):
            st.success("All workflows with recorded runs are healthy.")
    except Exception as exc:
        st.warning(f"Airflow run status is unavailable: {exc}")

gold_data_dir = Path(
    os.getenv("GOLD_DATA_DIR", "/opt/project-data/gold/hourly_measurements_v4")
)
lookback_days = int(os.getenv("DASHBOARD_LOOKBACK_DAYS", "30"))


@st.cache_data(ttl=60)
def load_gold_data(path, start_date):
    if os.getenv("PGHOST"):
        try:
            conninfo = make_conninfo(
                host=os.environ["PGHOST"],
                port=os.getenv("PGPORT", "5432"),
                dbname=os.environ["PGDATABASE"],
                user=os.environ["PGUSER"],
                password=os.environ["PGPASSWORD"],
                connect_timeout=5,
                application_name="egypt-air-quality-dashboard",
            )
            with psycopg.connect(conninfo) as conn:
                records = conn.execute(
                    POSTGRES_QUERY,
                    (date.fromisoformat(start_date),),
                ).fetchall()
            if records:
                return pd.DataFrame.from_records(records, columns=GOLD_COLUMNS), (
                    "PostgreSQL star-schema warehouse"
                )
            logger.warning("PostgreSQL warehouse is empty; using Gold Parquet.")
        except Exception:
            logger.exception("PostgreSQL query failed; using Gold Parquet.")

    root = Path(path)
    if not root.is_dir() or next(root.rglob("*.parquet"), None) is None:
        return pd.DataFrame(), "Gold Parquet (unavailable)"

    dataset = ds.dataset(root, format="parquet", partitioning="hive")
    scanner = dataset.scanner(
        columns=GOLD_COLUMNS + ["latitude_count", "longitude_count"],
        filter=ds.field("window_date") >= start_date,
        batch_size=25_000,
    )
    # Aggregate each Arrow batch before retaining it. This bounds peak memory
    # to one raw Parquet batch plus the much smaller set of hourly aggregates.
    partial_frames = []
    for record_batch in scanner.to_batches():
        batch_frame = record_batch.to_pandas()
        if not batch_frame.empty:
            partial_frames.append(merge_gold_batches(batch_frame))

    if not partial_frames:
        return pd.DataFrame(), "Gold Parquet"

    merged = merge_gold_batches(pd.concat(partial_frames, ignore_index=True))
    return merged, "Gold Parquet fallback"


if lookback_days <= 0:
    st.error("DASHBOARD_LOOKBACK_DAYS must be greater than zero.")
    st.stop()

start_date = (date.today() - timedelta(days=lookback_days - 1)).isoformat()
gold_data, data_source = load_gold_data(str(gold_data_dir), start_date)
st.caption(f"Data source: {data_source}")

if gold_data.empty:
    st.info(
        "No Gold aggregates are available yet. Start the Compose stack and "
        "wait for ingestion and Spark processing to complete."
    )
    st.stop()

st.sidebar.header("Filters")
sources = sorted(gold_data["source"].dropna().unique().tolist())
selected_sources = st.sidebar.multiselect(
    "Data sources",
    sources,
    default=sources,
)

filtered = gold_data[gold_data["source"].isin(selected_sources)]
filtered = filtered.copy()
filtered["governorate"] = filtered["governorate"].fillna("Unknown")
governorates = sorted(
    filtered["governorate"].dropna().unique().tolist()
)
selected_governorates = st.sidebar.multiselect(
    "Governorates",
    governorates,
    default=governorates,
)
filtered = filtered[
    filtered["governorate"].isin(selected_governorates)
]

metrics = sorted(filtered["metric"].dropna().unique().tolist())
selected_metrics = st.sidebar.multiselect(
    "Measurements",
    metrics,
    default=metrics,
)
filtered = filtered[filtered["metric"].isin(selected_metrics)]

if filtered.empty:
    st.warning("No measurements match the selected filters.")
    st.stop()

latest_window = filtered["window_start"].max()
latest_values = filtered.loc[
    filtered["window_start"] == latest_window,
    "average_value",
]
metric_col, records_col, window_col = st.columns(3)
metric_col.metric("Latest aggregate mean", f"{latest_values.mean():.2f}")
records_col.metric("Hourly groups", f"{len(filtered):,}")
window_col.metric("Latest hour (UTC)", str(latest_window))

st.subheader("Hourly trends")
trend = (
    filtered.pivot_table(
        index="window_start",
        columns="metric",
        values="average_value",
        aggfunc="mean",
    )
    .sort_index()
)
st.line_chart(trend)

st.subheader("Latest governorate measurements")
latest_by_governorate = (
    filtered.sort_values("window_start")
    .groupby(["governorate", "metric", "source"], dropna=False)
    .tail(1)
    .sort_values(["governorate", "metric"])
)
st.dataframe(
    latest_by_governorate[
        [
            "governorate",
            "source",
            "metric",
            "unit",
            "window_start",
            "average_value",
            "minimum_value",
            "maximum_value",
            "measurement_count",
        ]
    ],
    width="stretch",
    hide_index=True,
)

map_data = (
    latest_by_governorate[["latitude", "longitude"]]
    .dropna()
    .rename(columns={"latitude": "lat", "longitude": "lon"})
)
if not map_data.empty:
    st.subheader("Reporting locations")
    st.map(map_data)

if os.getenv("PGHOST"):
    st.subheader("Anomaly alerts — last 7 days")
    try:
        conninfo = make_conninfo(
            host=os.environ["PGHOST"],
            port=os.getenv("PGPORT", "5432"),
            dbname=os.environ["PGDATABASE"],
            user=os.environ["PGUSER"],
            password=os.environ["PGPASSWORD"],
            connect_timeout=5,
            application_name="egypt-air-quality-dashboard-alerts",
        )
        with psycopg.connect(conninfo) as conn:
            alerts = conn.execute(
                """
                SELECT
                    a.created_at,
                    a.window_start,
                    a.severity,
                    s.source_name AS source,
                    g.governorate_name AS governorate,
                    m.metric_name AS metric,
                    NULLIF(m.unit, '') AS unit,
                    a.observed_value,
                    a.baseline_median,
                    a.deviation_score,
                    CASE
                        WHEN a.telegram_sent_at IS NOT NULL THEN 'Sent'
                        ELSE 'Pending Telegram configuration'
                    END AS notification_status
                FROM anomaly_alerts AS a
                JOIN dim_source AS s ON s.source_key = a.source_key
                JOIN dim_governorate AS g ON g.governorate_key = a.governorate_key
                JOIN dim_metric AS m ON m.metric_key = a.metric_key
                WHERE a.created_at >= now() - interval '7 days'
                  AND a.suppressed_at IS NULL
                ORDER BY a.created_at DESC
                LIMIT 100
                """
            ).fetchall()
        if alerts:
            st.dataframe(
                pd.DataFrame.from_records(
                    alerts,
                    columns=[
                        "created_at", "window_start", "severity", "source",
                        "governorate", "metric", "unit", "observed_value",
                        "baseline_median", "deviation_score",
                        "notification_status",
                    ],
                ),
                width="stretch",
                hide_index=True,
            )
        else:
            st.info("No anomalies detected in the last seven days.")
    except Exception:
        logger.exception("Unable to load anomaly alerts.")
        st.warning("Anomaly alert history is temporarily unavailable.")

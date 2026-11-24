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
    table = dataset.to_table(
        filter=ds.field("window_date") >= start_date
    )
    frame = table.to_pandas()
    if frame.empty:
        return frame, "Gold Parquet"

    return merge_gold_batches(frame), "Gold Parquet fallback"


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

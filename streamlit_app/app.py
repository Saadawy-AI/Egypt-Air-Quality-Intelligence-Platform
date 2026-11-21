import os
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pyarrow.dataset as ds
import streamlit as st


st.set_page_config(
    page_title="Egypt Air Quality",
    layout="wide",
)
st.title("Egypt Air Quality Intelligence Platform")
st.caption("Hourly measurements aggregated from OpenAQ and Open-Meteo.")

gold_data_dir = Path(
    os.getenv("GOLD_DATA_DIR", "/opt/project-data/gold/hourly_measurements")
)
lookback_days = int(os.getenv("DASHBOARD_LOOKBACK_DAYS", "30"))


@st.cache_data(ttl=60)
def load_gold_data(path, start_date):
    root = Path(path)
    if not root.is_dir() or next(root.rglob("*.parquet"), None) is None:
        return pd.DataFrame()

    dataset = ds.dataset(root, format="parquet", partitioning="hive")
    table = dataset.to_table(
        filter=ds.field("window_date") >= start_date
    )
    return table.to_pandas()


if lookback_days <= 0:
    st.error("DASHBOARD_LOOKBACK_DAYS must be greater than zero.")
    st.stop()

start_date = (date.today() - timedelta(days=lookback_days - 1)).isoformat()
gold_data = load_gold_data(str(gold_data_dir), start_date)

if not gold_data.empty:
    window_times = pd.to_datetime(gold_data["window_start"], utc=True)
    now_utc = pd.Timestamp.now(tz="UTC")
    cutoff_utc = now_utc - pd.Timedelta(hours=72)

    gold_data = gold_data.loc[
        (window_times >= cutoff_utc) & (window_times <= now_utc)
    ].copy()

if gold_data.empty:
    st.info(
        "No measurements from the last 72 hours are available. "
        "Check ingestion and Spark processing."
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
    use_container_width=True,
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

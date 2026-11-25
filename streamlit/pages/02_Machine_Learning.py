import json
import logging
import os
from pathlib import Path
import pandas as pd
import psycopg
import streamlit as st
from psycopg.conninfo import make_conninfo

logger = logging.getLogger("EgyptAirQualityMLDashboard")
st.set_page_config(page_title="Machine Learning | Egypt Air Quality", page_icon="🧠", layout="wide")
st.title("Machine Learning Dashboard")
st.caption("PM2.5 model research, holdout evaluation, and actual-observation coverage.")

@st.cache_data(ttl=60)
def load_metadata(path):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8")), None
    except FileNotFoundError:
        return None, "Model metadata file is not available."
    except (OSError, json.JSONDecodeError) as exc:
        logger.exception("Unable to read model metadata.")
        return None, f"Metadata read failed ({type(exc).__name__})."

@st.cache_data(ttl=60)
def load_experiment_metadata(path):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8")), None
    except FileNotFoundError:
        return None, "No time-series experiment has been run yet."
    except (OSError, json.JSONDecodeError) as exc:
        logger.exception("Unable to read experiment metadata.")
        return None, f"Experiment metadata read failed ({type(exc).__name__})."

@st.cache_data(ttl=60)
def load_actuals():
    keys = ("PGHOST", "PGDATABASE", "PGUSER", "PGPASSWORD")
    if not all(os.getenv(k) for k in keys):
        return pd.DataFrame(), "PostgreSQL configuration is incomplete."
    try:
        conninfo = make_conninfo(host=os.environ["PGHOST"], port=os.getenv("PGPORT", "5432"),
            dbname=os.environ["PGDATABASE"], user=os.environ["PGUSER"],
            password=os.environ["PGPASSWORD"], connect_timeout=5,
            application_name="egypt-air-quality-ml-dashboard")
        with psycopg.connect(conninfo) as conn:
            rows = conn.execute("""
                SELECT f.window_start, g.governorate_name AS governorate,
                       f.average_value, f.measurement_count
                FROM fact_hourly_measurements f
                JOIN dim_source s ON s.source_key=f.source_key
                JOIN dim_governorate g ON g.governorate_key=f.governorate_key
                JOIN dim_metric m ON m.metric_key=f.metric_key
                WHERE s.source_name='openaq' AND m.metric_name='pm25'
                  AND g.governorate_name <> 'Unknown'
                  AND f.average_value IS NOT NULL AND f.average_value >= 0
                  AND f.window_start >= now() - interval '30 days'
                ORDER BY f.window_start
            """).fetchall()
        return pd.DataFrame(rows, columns=["window_start", "governorate", "average_value", "measurement_count"]), None
    except Exception as exc:
        logger.exception("Unable to load actual PM2.5 data.")
        return pd.DataFrame(), f"Actual-data query failed ({type(exc).__name__})."

experiment_path = os.getenv(
    "ML_EXPERIMENT_METADATA_PATH",
    "/opt/project-models/pm25/experiments/pm25_v3_candidate.json",
)
experiment, experiment_error = load_experiment_metadata(experiment_path)
st.header("Model experiments · training-only time-series cross-validation")
if experiment is None:
    st.info(experiment_error)
else:
    cv_results = experiment.get("cv_results", {})
    selected = experiment.get("selected_candidate", "N/A")
    holdout = experiment.get("holdout", {})
    holdout_model = holdout.get("model", {})
    holdout_base = holdout.get("persistence_baseline", {})
    cv_passed = bool(experiment.get("cv_gate_passed", False))
    holdout_passed = bool(holdout.get("baseline_gate_passed", False))
    passed = bool(experiment.get("overall_qualification_gate_passed", False))
    x, y, z, w = st.columns(4)
    x.metric("Selected experiment", selected)
    y.metric("CV folds", experiment.get("cv_split_count", 0))
    z.metric("CV gate", "Passed" if cv_passed else "Not qualified")
    w.metric("Locked holdout gate", "Passed" if holdout_passed else "Not qualified")
    if passed:
        st.success("The selected candidate beat persistence on both CV and locked-holdout MAE/RMSE. It remains research-only pending review.")
    else:
        st.warning("The candidate did not pass both the training-only CV and locked-holdout gates. No deployment is permitted.")

    coverage = experiment.get("temporal_coverage", [])
    if coverage:
        st.subheader("Observation continuity and data gaps")
        coverage_frame = pd.DataFrame(coverage)
        st.dataframe(
            coverage_frame[
                ["series", "feature_rows", "first_observation", "last_observation",
                 "maximum_gap_hours", "gaps_over_24h", "gaps_over_7d"]
            ],
            width="stretch",
            hide_index=True,
        )
        long_gaps = [
            {"series": item.get("series"), **gap}
            for item in coverage
            for gap in item.get("longest_gaps", [])
        ]
        if long_gaps:
            st.warning(
                "The training history contains long observation gaps. Missing hours "
                "were not interpolated; validation spans distinct collection periods "
                "and may reflect temporal distribution shift."
            )
            st.dataframe(
                pd.DataFrame(long_gaps).sort_values("duration_hours", ascending=False),
                width="stretch",
                hide_index=True,
            )

    rows = []
    for name, result in cv_results.items():
        cv, base = result.get("cv", {}), result.get("persistence_cv", {})
        rows.append({
            "Candidate": name,
            "Model": result.get("model_class", "Unknown"),
            "CV MAE": cv.get("mean_mae"),
            "CV MAE std": cv.get("std_mae"),
            "CV RMSE": cv.get("mean_rmse"),
            "CV RMSE std": cv.get("std_rmse"),
            "Persistence CV MAE": base.get("mean_mae"),
            "Persistence CV RMSE": base.get("mean_rmse"),
            "CV gate": "Pass" if result.get("cv_gate_passed") else "Fail",
        })
    if rows:
        results_frame = pd.DataFrame(rows).sort_values(["CV RMSE", "CV MAE"])
        st.dataframe(results_frame, width="stretch", hide_index=True)
        chart_frame = results_frame.set_index("Candidate")[["CV RMSE", "Persistence CV RMSE"]]
        st.subheader("Cross-validation RMSE")
        st.bar_chart(chart_frame)
    st.subheader("Final locked-holdout result")
    m1, m2, m3 = st.columns(3)
    for col, key, label in (
        (m1, "mae", "MAE (µg/m³)"),
        (m2, "rmse", "RMSE (µg/m³)"),
        (m3, "r2", "R²"),
    ):
        value, base = holdout_model.get(key), holdout_base.get(key)
        col.metric(label, f"{value:.3f}" if value is not None else "N/A",
                   delta=f"{value-base:+.3f} vs persistence" if value is not None and base is not None else None,
                   delta_color="inverse" if key in ("mae", "rmse") else "normal")
    st.caption(
        f"Holdout: {holdout.get('start', 'N/A')} → {holdout.get('end', 'N/A')} · "
        f"{holdout.get('rows', 0):,} rows. Candidate selected using training-only CV; "
        "the locked holdout was evaluated once after selection."
    )

    diagnostics = holdout.get("diagnostics", {})
    st.subheader("Holdout error diagnostics")
    q1, q2, q3, q4 = st.columns(4)
    q1.metric("Median absolute error", f"{diagnostics.get('absolute_error_p50', float('nan')):.2f} µg/m³")
    q2.metric("P90 absolute error", f"{diagnostics.get('absolute_error_p90', float('nan')):.2f} µg/m³")
    q3.metric("P95 absolute error", f"{diagnostics.get('absolute_error_p95', float('nan')):.2f} µg/m³")
    q4.metric("Mean signed error", f"{diagnostics.get('mean_signed_error', float('nan')):+.2f} µg/m³")

    hourly_errors = pd.DataFrame(diagnostics.get("by_hour_of_day_utc", []))
    if not hourly_errors.empty:
        st.caption("Error by UTC hour; displayed to identify systematic time-of-day weaknesses.")
        st.line_chart(
            hourly_errors.set_index("hour_of_day_utc")[
                ["model_mae", "persistence_mae", "model_rmse", "persistence_rmse"]
            ]
        )

    monthly_errors = pd.DataFrame(diagnostics.get("by_month_utc", []))
    if not monthly_errors.empty:
        st.subheader("Temporal drift · monthly holdout errors")
        st.caption("Only months with actual holdout observations are shown; missing months are not interpolated.")
        st.bar_chart(
            monthly_errors.set_index("month_utc")[
                ["model_mae", "persistence_mae", "model_rmse", "persistence_rmse"]
            ]
        )
        st.dataframe(monthly_errors, width="stretch", hide_index=True)

    prediction_path = Path(experiment_path).with_name(
        holdout.get("prediction_artifact", "pm25_v3_holdout_predictions.csv")
    )
    try:
        if prediction_path.is_file():
            holdout_predictions = pd.read_csv(prediction_path, parse_dates=["window_start"])
            if not holdout_predictions.empty:
                st.subheader("Actual vs predicted · locked holdout")
                st.line_chart(
                    holdout_predictions.set_index("window_start")[
                        ["actual", "model_prediction", "persistence_prediction"]
                    ]
                )
                st.subheader("Largest candidate errors")
                st.dataframe(
                    holdout_predictions.nlargest(10, "model_absolute_error")[
                        ["window_start", "series", "actual", "model_prediction",
                         "persistence_prediction", "model_absolute_error"]
                    ],
                    width="stretch",
                    hide_index=True,
                )
                st.download_button(
                    "Download holdout predictions CSV",
                    data=prediction_path.read_bytes(),
                    file_name=prediction_path.name,
                    mime="text/csv",
                )
        else:
            st.info("Holdout prediction detail file is not available; aggregate diagnostics remain visible.")
    except (OSError, ValueError, KeyError) as exc:
        logger.exception("Unable to load holdout prediction diagnostics.")
        st.warning(f"Holdout prediction detail unavailable ({type(exc).__name__}).")

    with st.expander("Experiment configuration and fold results"):
        st.json({
            "selection_rule": experiment.get("selection_rule"),
            "selected_parameters": experiment.get("selected_parameters"),
            "cv_folds": cv_results.get(selected, {}).get("folds", []),
            "production_deployed": experiment.get("production_deployed", False),
            "candidate_status": experiment.get("candidate_status"),
        })

st.divider()
recent_experiment_path = os.getenv(
    "ML_RECENT_EXPERIMENT_METADATA_PATH",
    "/opt/project-models/pm25/experiments/recent_regime/pm25_v4_recent_regime.json",
)
recent_experiment, recent_experiment_error = load_experiment_metadata(recent_experiment_path)
st.header("Recent-regime experiment · current collection period")
if recent_experiment is None:
    st.info(recent_experiment_error)
else:
    recent_holdout = recent_experiment.get("holdout", {})
    recent_model = recent_holdout.get("model", {})
    recent_baseline = recent_holdout.get("persistence_baseline", {})
    recent_cv = recent_experiment.get("cv_results", {}).get(
        recent_experiment.get("selected_candidate", ""), {}
    )
    recent_cv_metrics = recent_cv.get("cv", {})
    recent_cv_baseline = recent_cv.get("persistence_cv", {})
    st.caption(
        "This experiment excludes historical regimes before the latest >24-hour "
        "raw-observation gap for each governorate. It is evaluated independently "
        "from the full-history candidate above."
    )
    a, b, c, d = st.columns(4)
    a.metric("Selected model", recent_experiment.get("model_class", "N/A"))
    b.metric("Recent feature rows", f"{recent_experiment.get('training', {}).get('feature_rows', 0):,}")
    c.metric("Holdout rows", f"{recent_holdout.get('rows', 0):,}")
    d.metric("Qualification", "Passed" if recent_experiment.get("overall_qualification_gate_passed") else "Not qualified")
    if recent_experiment.get("overall_qualification_gate_passed"):
        st.success("Recent-regime candidate passed both CV and locked-holdout gates; it remains research-only pending review.")
    else:
        st.warning("Recent-regime candidate did not beat persistence on both MAE and RMSE across CV and locked holdout. It is not deployable.")
    coverage_rows = recent_experiment.get("regime_coverage", [])
    if coverage_rows:
        st.dataframe(pd.DataFrame(coverage_rows), width="stretch", hide_index=True)
    st.subheader("Recent-regime metrics · model vs persistence")
    metrics_rows = []
    for period, candidate_metrics, baseline_metrics in (
        ("Training-only CV", recent_cv_metrics, recent_cv_baseline),
        ("Locked holdout", recent_model, recent_baseline),
    ):
        metrics_rows.append({
            "Evaluation": period,
            "Model MAE": candidate_metrics.get("mean_mae", candidate_metrics.get("mae")),
            "Persistence MAE": baseline_metrics.get("mean_mae", baseline_metrics.get("mae")),
            "Model RMSE": candidate_metrics.get("mean_rmse", candidate_metrics.get("rmse")),
            "Persistence RMSE": baseline_metrics.get("mean_rmse", baseline_metrics.get("rmse")),
        })
    st.dataframe(pd.DataFrame(metrics_rows), width="stretch", hide_index=True)
    st.caption(
        f"Training window: {recent_experiment.get('training', {}).get('start', 'N/A')} → "
        f"{recent_experiment.get('training', {}).get('end', 'N/A')} · "
        f"holdout: {recent_holdout.get('start', 'N/A')} → {recent_holdout.get('end', 'N/A')}"
    )

st.divider()
weather_experiment_path = os.getenv(
    "ML_WEATHER_EXPERIMENT_METADATA_PATH",
    "/opt/project-models/pm25/experiments/weather_reanalysis/pm25_weather_reanalysis_v1.json",
)
weather_experiment, weather_experiment_error = load_experiment_metadata(weather_experiment_path)
st.header("Weather-informed experiment · historical reanalysis")
if weather_experiment is None:
    st.info(weather_experiment_error)
else:
    weather_holdout = weather_experiment.get("holdout", {})
    weather_model = weather_holdout.get("model", {})
    weather_baseline = weather_holdout.get("persistence_baseline", {})
    weather_selected = weather_experiment.get("selected_candidate", "N/A")
    weather_cv = weather_experiment.get("cv_results", {}).get(weather_selected, {})
    weather_cv_metrics = weather_cv.get("cv", {})
    weather_cv_baseline = weather_cv.get("persistence_cv", {})
    weather_gate = bool(weather_experiment.get("overall_qualification_gate_passed", False))
    st.caption(
        "Research-only experiment joining actual OpenAQ PM2.5 targets to Open-Meteo "
        "historical reanalysis. Weather predictors are shifted by at least one hour; "
        "target-hour weather is never used. Reanalysis is modelled weather, not a "
        "local station observation or an archived forecast vintage."
    )
    a, b, c, d = st.columns(4)
    a.metric("Selected model", weather_experiment.get("model_class", "N/A"))
    b.metric("Aligned rows", f"{weather_experiment.get('training', {}).get('model_rows_after_lag_alignment', 0):,}")
    c.metric("Holdout rows", f"{weather_holdout.get('rows', 0):,}")
    d.metric("Qualification", "Passed" if weather_gate else "Not qualified")
    if weather_gate:
        st.success("Weather-informed candidate passed both CV and locked-holdout gates; it remains research-only pending review.")
    elif weather_holdout.get("baseline_gate_passed"):
        st.warning("Holdout MAE/RMSE improved, but training-only CV did not beat persistence. The candidate is not qualified and is not deployable.")
    else:
        st.warning("The weather-informed candidate did not pass both qualification gates. It is not deployable.")
    weather_metrics_rows = []
    for period, candidate_metrics, baseline_metrics in (
        ("Training-only CV", weather_cv_metrics, weather_cv_baseline),
        ("Locked holdout", weather_model, weather_baseline),
    ):
        weather_metrics_rows.append({
            "Evaluation": period,
            "Model MAE": candidate_metrics.get("mean_mae", candidate_metrics.get("mae")),
            "Persistence MAE": baseline_metrics.get("mean_mae", baseline_metrics.get("mae")),
            "Model RMSE": candidate_metrics.get("mean_rmse", candidate_metrics.get("rmse")),
            "Persistence RMSE": baseline_metrics.get("mean_rmse", baseline_metrics.get("rmse")),
        })
    st.dataframe(pd.DataFrame(weather_metrics_rows), width="stretch", hide_index=True)
    st.caption(
        f"Training: {weather_experiment.get('training', {}).get('start', 'N/A')} → "
        f"{weather_experiment.get('training', {}).get('end', 'N/A')} · "
        f"holdout: {weather_holdout.get('start', 'N/A')} → {weather_holdout.get('end', 'N/A')}"
    )
    weather_prediction_path = Path(weather_experiment_path).with_name(
        weather_holdout.get("prediction_artifact", "pm25_weather_reanalysis_v1_holdout_predictions.csv")
    )
    if weather_prediction_path.is_file():
        weather_predictions = pd.read_csv(
            weather_prediction_path, parse_dates=["window_start"]
        )
        if not weather_predictions.empty:
            st.subheader("Weather-informed model · locked holdout predictions")
            st.line_chart(
                weather_predictions.set_index("window_start")[
                    ["actual", "model_prediction", "persistence_prediction"]
                ]
            )
            st.download_button(
                "Download weather-informed holdout CSV",
                data=weather_prediction_path.read_bytes(),
                file_name=weather_prediction_path.name,
                mime="text/csv",
            )
    weather_meta = weather_experiment.get("weather", {})
    retrieval = weather_meta.get("retrieval_metadata", {})
    st.write({
        "Weather source": weather_meta.get("provider"),
        "Dataset": weather_meta.get("dataset"),
        "Attribution": weather_meta.get("attribution"),
        "Weather interval": f"{retrieval.get('start_date', 'N/A')} → {retrieval.get('end_date', 'N/A')}",
        "Weather rows": retrieval.get("rows", 0),
        "Weather missing values": retrieval.get("null_counts", {}),
    })
    with st.expander("Weather feature contract and experiment configuration"):
        st.write({
            "Target-hour weather used": weather_meta.get("target_hour_weather_used", True),
            "Weather features shifted": weather_meta.get("features_use_only_weather_at_t_minus_1_and_prior_3h", False),
            "Forecast vintage available": weather_meta.get("forecast_vintage_available", False),
            "Production deployed": weather_experiment.get("production_deployed", False),
            "Candidate status": weather_experiment.get("candidate_status"),
        })
        st.code("\n".join(weather_experiment.get("feature_columns", [])), language="text")

metadata_path = os.getenv("ML_METADATA_PATH", "/opt/project-models/pm25/pm25_hgb_v2.json")
metadata, error = load_metadata(metadata_path)
if metadata is None:
    st.warning(error)
else:
    evaluation = metadata.get("evaluation", {})
    model = evaluation.get("model", {})
    baseline = evaluation.get("naive_persistence_baseline", {})
    passed = bool(evaluation.get("baseline_gate_passed", False))
    a, b, c = st.columns(3)
    a.metric("Deployment gate", "Passed" if passed else "Not qualified")
    b.metric("Candidate", metadata.get("model_version", "Unknown"))
    c.metric("Trained at (UTC)", str(metadata.get("trained_at_utc", "Unknown"))[:19].replace("T", " "))
    if passed:
        st.success("Offline evaluation gate passed. Deployment still requires a separate review.")
    else:
        st.warning("Research candidate only — NOT deployed. It failed the required MAE + RMSE comparison against persistence.")

    st.subheader("Chronological holdout evaluation · one-hour-ahead")
    cols = st.columns(3)
    for col, key, label, unit in zip(cols, ("mae", "rmse", "r2"), ("MAE", "RMSE", "R²"), ("µg/m³", "µg/m³", "")):
        value, base = model.get(key), baseline.get(key)
        col.metric(label, f"{value:.3f} {unit}" if value is not None else "N/A",
            delta=f"{value-base:+.3f} vs persistence" if value is not None and base is not None else None,
            delta_color="inverse" if key in ("mae", "rmse") else "normal")
    if model and baseline:
        st.bar_chart(pd.DataFrame({
            "HGB candidate": [model.get("mae"), model.get("rmse")],
            "Persistence": [baseline.get("mae"), baseline.get("rmse")],
        }, index=["MAE (µg/m³)", "RMSE (µg/m³)"]))
        st.caption(f"R² — candidate: {model.get('r2', float('nan')):.3f}; persistence: {baseline.get('r2', float('nan')):.3f}. Lower is better for MAE/RMSE; higher is better for R². Qualification requires both MAE and RMSE to improve.")

    cols = st.columns(4)
    cols[0].metric("Raw OpenAQ rows", f"{metadata.get('raw_rows', 0):,}")
    cols[1].metric("Feature rows", f"{metadata.get('feature_rows', 0):,}")
    cols[2].metric("Train rows", f"{evaluation.get('train_rows', 0):,}")
    cols[3].metric("Holdout rows", f"{evaluation.get('test_rows', 0):,}")
    st.subheader("Training coverage and methodology")
    st.write({
        "Geographic scope": metadata.get("geographic_scope", "N/A"),
        "Target": metadata.get("target", "N/A"),
        "Horizon": metadata.get("prediction_horizon", "N/A"),
        "Training period": f"{evaluation.get('train_start', 'N/A')} → {evaluation.get('train_end', 'N/A')}",
        "Holdout period": f"{evaluation.get('test_start', 'N/A')} → {evaluation.get('test_end', 'N/A')}",
        "Governorate series": ", ".join(evaluation.get("series", [])) or "N/A",
    })
    with st.expander("Feature contract"):
        st.write("Lagged observations and calendar features only. Missing hours remain missing; no interpolation.")
        st.code("\n".join(metadata.get("feature_columns", [])), language="text")
    st.subheader("Deployment decision")
    st.markdown("- Training uses actual OpenAQ PM2.5 observations only.\n- Open-Meteo forecast values and unresolved governorates are excluded.\n- Evaluation uses a chronological holdout, not a random split.\n- Production inference remains disabled until both MAE and RMSE beat persistence and a fresh review passes.")

actuals, actuals_error = load_actuals()
st.subheader("Recent actual PM2.5 observations · last 30 days")
if actuals_error:
    st.warning(actuals_error)
elif actuals.empty:
    st.info("No resolved actual OpenAQ PM2.5 observations found in the last 30 days.")
else:
    actuals["window_start"] = pd.to_datetime(actuals["window_start"], utc=True)
    latest = actuals.sort_values("window_start").groupby("governorate").tail(1)
    a, b, c = st.columns(3)
    a.metric("Hourly aggregates", f"{len(actuals):,}")
    b.metric("Governorates", f"{actuals['governorate'].nunique():,}")
    c.metric("Latest observation (UTC)", actuals["window_start"].max().strftime("%Y-%m-%d %H:%M"))
    st.line_chart(actuals.pivot_table(index="window_start", columns="governorate", values="average_value", aggfunc="mean").sort_index())
    st.dataframe(latest[["governorate", "window_start", "average_value", "measurement_count"]].sort_values("governorate"), width="stretch", hide_index=True)

st.caption("No live predictions are displayed: the current candidate is not qualified for production deployment.")

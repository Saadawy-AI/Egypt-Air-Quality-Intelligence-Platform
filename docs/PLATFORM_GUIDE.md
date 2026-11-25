# Egypt Air Quality Intelligence Platform — Infrastructure

Local development infrastructure:
- PostgreSQL (two databases: `airflow`, `airquality`)
- Apache Kafka 4.x in single-node KRaft mode
- Eclipse Mosquitto MQTT broker
- Apache Spark standalone master + worker
- Apache Airflow 3.0.6 (LocalExecutor) for hourly ingestion
- MQTT-to-Kafka bridge for ESP32 particulate sensors
- Spark Bronze, validated Silver, and incremental Gold Parquet layers
- PostgreSQL star-schema warehouse and idempotent Gold loader
- Robust median/MAD anomaly detection with deduplicated Telegram alerts
- Streamlit dashboard backed by the PostgreSQL serving layer

Azure is intentionally not emulated in Docker. The cloud data lake target is Azure Data Lake Storage Gen2; connect it in a later cloud-integration step. Local project files are mounted under `./data`.

## Prerequisites
Docker Desktop with WSL2 integration enabled, and Docker Compose v2.

## First-time setup (run from this directory in WSL Ubuntu)

1. Copy environment template:
   `cp .env.example .env`

2. Generate an Airflow Fernet key:
   `docker run --rm apache/airflow:3.0.6 python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`

3. Put the generated key in `.env` as `AIRFLOW_FERNET_KEY`. Generate a separate
   API JWT secret in `.env.airflow-jwt`:
   `printf 'AIRFLOW__API_AUTH__JWT_SECRET=%s\n' "$(openssl rand -hex 64)" > .env.airflow-jwt`
   Add an independent web-session signing key:
   `printf 'AIRFLOW__API__SECRET_KEY=%s\n' "$(openssl rand -hex 64)" >> .env.airflow-jwt`
   Replace every sample password with a strong unique password.

4. Make the init script executable:
   `chmod +x postgres/init/01-create-databases.sh`

5. Start infrastructure:
   `docker compose up -d --build`

6. Check services:
   `docker compose ps -a`

The default Compose stack creates the Kafka topics and starts the Bronze, Silver,
and Gold Spark streams. Airflow runs the `egypt_air_quality_ingestion` DAG
every hour. The DAG fetches, normalizes, validates, and publishes records to
Kafka. Spark Bronze consumes Kafka and writes the raw Kafka envelopes to
Parquet; Silver validates and separates accepted and rejected records before
Gold aggregation.

For an optional one-time bootstrap ingestion (kept out of normal
`docker compose up` so repeated stack starts do not republish the same
initial data), run:

`docker compose run --rm ingestion`

Set `OPENAQ_API_KEY` in `.env` to enable OpenAQ; Open-Meteo ingestion does not
require an API key.

`docker compose logs -f airflow-scheduler mqtt-kafka-bridge spark-bronze spark-silver spark-gold postgres-loader`

The local lake is organized as:
- Bronze raw Kafka records: `./data/bronze`
- Validated Silver records: `./data/silver`
- Rejected records with validation reasons: `./data/quarantine`
- Incremental Gold batch aggregates: `./data/gold/hourly_measurements_v4` (each `batch_id` is an idempotent contribution; the dashboard merges partial aggregates by hour and metric)
- Spark streaming state: `./data/checkpoints` (Bronze, Silver, and Gold
  checkpoints have separate subdirectories)

## Incremental Silver-to-Gold handoff

Silver writes accepted rows from each non-empty micro-batch to a deterministic
`batch_id` directory and stores rejected rows separately in Quarantine. After
the valid Silver Parquet commit succeeds, it publishes a compact event to Kafka
topic `silver-batch-notifications` containing the dataset, Silver batch ID,
validated row count, and batch path. The notification topic retains events for
30 days to provide a recovery window if Gold is temporarily unavailable.

Gold consumes notifications from Kafka using its independent
`gold_notifications_v5` checkpoint; it does not recursively rescan the Silver
lake. Before aggregation, Gold validates the dataset and batch ID, derives the
expected path, checks the Silver `_SUCCESS` marker, and verifies that the
actual Parquet row count matches the notification. Gold uses deterministic
dataset-specific batch ID ranges so retries overwrite the same contribution.
The PostgreSQL loader then loads only committed Gold batches and records each
loaded batch in `processed_gold_batches` in the same transaction as its fact
upserts. Preserve all Bronze, Silver, and Gold checkpoints and the Kafka
notification topic during recovery; do not reset offsets or delete historical
batches as a routine troubleshooting step.

The `postgres-loader` service continuously loads committed Gold batches into
the `airquality` PostgreSQL database. It creates a star schema consisting of
`dim_source`, `dim_governorate`, `dim_metric`, `dim_date`, and
`fact_hourly_measurements`. The `processed_gold_batches` table records
successfully committed batch IDs; each batch load and its marker are written
in one database transaction, so retries do not double-count observations.
Only Spark batch directories containing both Parquet data and `_SUCCESS` are
eligible for loading.

## Anomaly detection and Telegram alerts

The `anomaly-detector` service evaluates the latest completed-hour warehouse
aggregates every two minutes. It compares each source/governorate/metric series
with up to eight days of prior observations using the median and median
absolute deviation (MAD), a minimum baseline size, and metric-specific floors.
Forecast rows and the still-open current hour are excluded. Circular wind-direction values are also excluded from linear anomaly scoring. Alerts are persisted in
`anomaly_alerts` with a unique series/hour key, so polling and container restarts
do not create duplicate alerts. Configure `TELEGRAM_BOT_TOKEN` and
`TELEGRAM_CHAT_ID` in the ignored local `.env` to enable delivery; without both
values, alerts remain stored and pending rather than being discarded.
Thresholds can be tuned with `ANOMALY_POLL_SECONDS`,
`ANOMALY_LOOKBACK_DAYS`, `ANOMALY_MIN_BASELINE_POINTS`,
`ANOMALY_MAD_MULTIPLIER`, and `ANOMALY_RELATIVE_FLOOR`.

The Streamlit application provides two dashboards in the sidebar:

- **Analysis Dashboard** (`streamlit_app/Analysis_Dashboard.py`, backed by `streamlit_app/app.py`): reads the PostgreSQL star-schema warehouse as its primary serving source and falls back to Gold Parquet. It includes source/governorate/metric filters, hourly trends, latest governorate readings, reporting locations, and recent anomaly alerts.
- **Machine Learning Dashboard** (`streamlit_app/pages/02_Machine_Learning.py`): presents model-family/CV comparisons, locked-holdout metrics against persistence, holdout prediction diagnostics (actual vs predicted, error quantiles, signed bias, UTC-hour error profile, largest errors, CSV export), training/holdout coverage, feature contract, and recent actual OpenAQ observations. It reads candidate metadata and research artifacts from `models/pm25/` and does not expose live production forecasts while candidates remain unqualified.

The primary warehouse and model-metadata reads are cached for 60 seconds. The analysis lookback window can be set with `DASHBOARD_LOOKBACK_DAYS` in `.env`. The Streamlit container mounts `./models` read-only so the ML page can inspect evaluation metadata without loading or deploying the candidate model.

Example warehouse query:

```sql
SELECT
    d.date_value,
    s.source_name,
    g.governorate_name,
    m.metric_name,
    m.unit,
    f.window_start,
    f.average_value,
    f.minimum_value,
    f.maximum_value,
    f.measurement_count
FROM fact_hourly_measurements AS f
JOIN dim_date AS d ON d.date_key = f.date_key
JOIN dim_source AS s ON s.source_key = f.source_key
JOIN dim_governorate AS g ON g.governorate_key = f.governorate_key
JOIN dim_metric AS m ON m.metric_key = f.metric_key
ORDER BY f.window_start DESC;
```

## OpenAQ historical PM2.5 backfill

OpenAQ hourly history is collected through the sensor `/hours` endpoint. Do not
publish the full history to the normal Kafka stream: its seven-day event-time
watermark is designed for recent ingestion and would discard older observations.

Export validated history to the local staging area (dry-run is the default):

`docker compose exec -T airflow-scheduler python -m ingestion.backfill_openaq --export-jsonl /opt/project-data/staging/openaq_pm25_historical.jsonl`

Preview the deduplicated Silver load:

`docker compose exec -T airflow-scheduler python -m ingestion.load_openaq_historical_silver --input /opt/project-data/staging/openaq_pm25_historical.jsonl`

Write the immutable historical batch using the shared Spark file owner:

`docker compose exec -T -u 185 airflow-scheduler python -m ingestion.load_openaq_historical_silver --input /opt/project-data/staging/openaq_pm25_historical.jsonl --write`

The loader reconstructs the same stable OpenAQ `record_id` used by Silver,
excludes IDs already present in Silver, rejects conflicting duplicate IDs, and
refuses to overwrite an existing batch directory. Gold consumes the new Silver
Parquet file through its existing streaming source and the PostgreSQL loader
processes the resulting committed Gold batch. This path preserves all existing
Bronze/Silver/Gold checkpoints and does not replay old timestamps through the
seven-day streaming watermark.

## PM2.5 machine-learning pipeline

The optional `ml-trainer` profile trains a one-hour-ahead PM2.5 model from
actual OpenAQ observations in PostgreSQL. Open-Meteo forecast rows are excluded.
The current training scope is Cairo, the only governorate with enough contiguous
resolved PM2.5 history for lag-based features. Alexandria currently has only one
resolved OpenAQ PM2.5 fact and is insufficient for a separate series. The SQL
source filter continues to exclude any `Unknown` governorate rows. The feature builder reindexes each governorate
to an hourly UTC timeline,
leaves missing hours missing, and uses lagged/rolling features only; no current
or future target values enter the feature set.

Run the reproducible training/evaluation job:

`docker compose --profile ml build ml-trainer`

`docker compose --profile ml run --rm ml-trainer`

Evaluation uses a global chronological 80/20 cutoff across the known-
governorate series and compares a HistGradientBoostingRegressor against a lag-1 persistence baseline
using MAE, RMSE, and R². The output under `models/pm25/` contains the candidate
model and JSON metadata. The metadata records whether the candidate beats the
baseline on both MAE and RMSE; it is never deployed automatically. The latest
run after resolving governorates used 20,032 actual OpenAQ PM2.5 rows and 16,456
feature rows (13,164 train / 3,292 test). Test-period MAE was 8.64 µg/m³, RMSE
14.12 µg/m³, and R² 0.820; the lag-1 baseline achieved MAE 8.51 µg/m³, RMSE
14.82 µg/m³, and R² 0.802. The candidate did not pass the two-metric gate because
its MAE was slightly worse than persistence, despite better RMSE. It remains a
research artifact only and is not deployed. This is a rolling one-step-ahead
evaluation, not a multi-hour recursive forecast.

A separate experiment runner compares five candidates across three model
families (HistGradientBoosting, RandomForest, and ExtraTrees) using four
expanding-window CV folds on the training period. It selects by mean CV RMSE
(MAE breaks ties), then evaluates the selected configuration once on the locked
holdout. Run it with:

`docker compose --profile ml run --rm ml-trainer python -m ml.experiment_pm25`

The experiment writes to `models/pm25/experiments/pm25_v3_candidate.*` and
never overwrites v2. On the latest run (20,033 raw observations; 16,457 feature
rows), RandomForest was selected by CV RMSE. Its mean CV MAE was 10.48 and RMSE
16.56, versus persistence CV MAE 10.18 and RMSE 16.39. On the locked holdout,
candidate MAE was 8.672 versus persistence 8.510; candidate RMSE was 14.028
versus persistence 14.815. Neither the CV nor locked-holdout two-metric gate
passed, so v3 is **not qualified and not deployed**. The runner also exports
the locked-holdout predictions to `models/pm25/experiments/pm25_v3_holdout_predictions.csv` and
records absolute-error quantiles, signed bias, error by UTC hour, and error by
governorate in the experiment metadata. The ML dashboard displays actual vs
predicted values, persistence, time-of-day error diagnostics, the ten largest
candidate errors, and a downloadable holdout CSV. The latest holdout diagnostics
show median absolute error 5.67 µg/m³, P90 19.00, P95 27.16, and mean signed
error -1.85 µg/m³ (the candidate overpredicts on average). Coverage analysis
also found 34 gaps longer than 24 hours, including an approximately 13,421-hour
(559-day) gap from 2025-03-04 to 2026-09-14; 11 gaps exceed seven days. The
training period ends on 2024-12-19, while recent 2026 holdout months show
candidate MAE worse than persistence (September 5.86 vs 4.40, October 6.19 vs
5.10, November 7.80 vs 6.80 µg/m³) and a negative signed error of roughly
-3.1 to -3.7 µg/m³. This is evidence of stale-regime bias after the long
collection gap. The dashboard surfaces the gap intervals and monthly drift.
This diagnostic visibility does not change the failed qualification gates or
enable deployment.

A second, isolated experiment now trains only on the latest contiguous raw
observation regime per governorate (segments are split at raw gaps greater than
24 hours), without changing the full-history v3 experiment. Reproduce it with:

`docker compose --profile ml run --no-deps --rm -e ML_EXPERIMENT_LATEST_REGIME=true -e ML_EXPERIMENT_VERSION=pm25_v4_recent_regime -e ML_EXPERIMENT_OUTPUT_DIR=/opt/project-models/pm25/experiments/recent_regime ml-trainer python -m ml.experiment_pm25`

The latest Cairo
regime begins 2026-09-07 17:00 UTC and ends 2026-11-23 05:00 UTC; it contains
1,814 raw hourly measurements and 1,544 engineered feature rows. Four expanding
CV folds compare the same five candidate models. ExtraTrees was selected, but
its CV MAE/RMSE were 5.558/7.528 µg/m³ versus persistence 5.185/7.387. On the
309-row locked holdout (2026-11-07 through 2026-11-23), its MAE/RMSE were
8.219/14.796 versus persistence 7.497/13.789. The recent-regime candidate also
fails both qualification gates and remains research-only. Its artifacts are
stored separately under
`models/pm25/experiments/recent_regime/pm25_v4_recent_regime.*`, and the
Machine Learning Dashboard now displays this recent-regime result alongside
the full-history experiment. Recent data availability has resumed, but the
current models still do not beat the simple persistence baseline; more recent
observations and better exogenous predictors are needed before deployment.

### Weather-informed PM2.5 experiment (research-only)

The existing streaming weather source calls the Open-Meteo Forecast API with
`forecast_days=2`; its short rolling window is not a historical training set.
To evaluate weather covariates without using forecast values as ground truth,
a separate research fetcher now retrieves historical weather reanalysis from
the Open-Meteo Historical Weather API. It writes only to
`data/staging/research/` and never publishes to Kafka, Bronze, Silver, Gold,
or PostgreSQL. The fetched Cairo window (2026-09-07 through 2026-11-18)
contains 1,752 hourly records and eight weather variables with no missing
values. The dataset is Open-Meteo best-match historical reanalysis, not local
station observations; attribution is Weather data by Open-Meteo.com (CC BY 4.0).

Re-fetch the research-only weather file with:

`docker compose exec airflow-api-server python -m ingestion.fetch_historical_weather --governorate Cairo --start-date 2026-09-07 --end-date 2026-11-18 --model best_match --output /opt/project-data/staging/research/openaq_weather_reanalysis_cairo_best_match_20260907_20261118.csv`

The weather-informed experiment joins actual OpenAQ PM2.5 targets to weather
by exact governorate and UTC hour. It uses only weather values available at
least one hour before the target (plus a three-hour trailing mean), never the
target-hour weather. Reproduce it with:

`docker compose --profile ml run --no-deps --rm ml-trainer python -m ml.experiment_pm25_weather`

The aligned dataset contains 1,457 rows (1,165 train / 292 locked holdout).
RandomForest was selected using four expanding-window CV folds. CV MAE/RMSE
were 5.508/7.463 µg/m³ versus persistence 5.050/7.172, so the CV gate failed.
On the locked holdout (2026-11-04 13:00 through 2026-11-18 23:00 UTC), model
MAE/RMSE were 6.073/10.199 versus persistence 6.429/11.850; the holdout gate
passed. Because training-only CV did not beat persistence, the overall gate
failed and this candidate remains **not qualified and not deployed**. Weather
coverage currently ends on 2026-11-18 (the historical API has a publication
delay), so the evaluation does not cover the final days of the PM2.5 holdout.
Artifacts are isolated under
`models/pm25/experiments/weather_reanalysis/`. The ML dashboard now displays
the weather-informed result, source attribution, temporal coverage, and the
separate CV/holdout decisions. No production model alias or live predictions
were changed.

## ESP32 / MQTT sensor ingestion

ESP32 gateways publish JSON measurements to the local Mosquitto broker on
`egypt/air-quality/sensors`. The `mqtt-kafka-bridge` subscribes to
`egypt/air-quality/#` and forwards the original payload to the existing
Kafka `air-quality` topic, using `metadata.device_id` as the Kafka key.
The bridge uses MQTT QoS 1 manual acknowledgements and acknowledges a message
only after Kafka confirms the publish. Keeping the existing Kafka topic means
the current Bronze, Silver, and Gold checkpoints remain unchanged.

Use `ingestion.mqtt.publisher.build_sensor_message` to construct the canonical
payload. The required fields are `source=esp32_pm_sensor`,
`country_code=EG`, a device ID, an Egypt-bounded location, a timestamp, and
at least one of `pm1`, `pm25`, or `pm10` in `data`. Pollutant values must
be finite and non-negative. Example:

```json
{
  "schema_version": "1.0",
  "source": "esp32_pm_sensor",
  "country_code": "EG",
  "governorate": "Cairo",
  "governorate_ar": "القاهرة",
  "latitude": 30.0444,
  "longitude": 31.2357,
  "timestamp": "2026-11-23T04:00:00Z",
  "data": {
    "pm1": "3.0",
    "pm25": "12.5",
    "pm10": "20.0"
  },
  "metadata": {
    "device_id": "device-01",
    "firmware_version": "1.2.0"
  }
}
```

Silver validates the sensor source/topic pairing, device ID, coordinates,
timestamp, numeric measurements, and non-negative particulate values. Invalid
payloads are routed to Quarantine with a validation reason. No synthetic
sensor measurements are published by the platform itself.

## Local readiness checks before Azure

1. Run `docker compose config -q` to validate the Compose configuration.
2. Start the stack and confirm services are healthy or running with
   `docker compose ps -a`, including `mqtt-kafka-bridge` and
   `postgres-loader`.
3. Confirm the Airflow DAG succeeds and records are present in Kafka.
4. Confirm the MQTT bridge subscribes to `egypt/air-quality/#`; when an ESP32
   is connected, verify its readings reach the `air-quality` Kafka topic.
5. Confirm new Parquet files appear under Bronze and Silver, and invalid
   records (if any) are written to Quarantine with a reason.
6. Confirm Gold hourly aggregates are loaded into
   `fact_hourly_measurements` and the dashboard reports PostgreSQL as its
   active data source.
7. Restart Spark services and confirm checkpoints resume processing without
   duplicating previously ingested observations.

Azure integration should replace the local lake paths with ADLS Gen2
locations and use managed identity or another approved secret-free workload
identity. Keep the same Bronze/Silver/Quarantine/Gold data contract.

## Optional direct raw export (outside the streaming pipeline)

The normal production path in this local stack is Airflow -> Kafka -> Spark
Bronze -> Silver -> Gold. The command below is a separate raw export utility;
its `raw-*.parquet` files are intentionally not consumed by the Silver stream,
which reads Spark-generated `part-*.parquet` files.

The Bronze layer is raw ingestion data. It is not normalized, validated,
Silver, or aggregated Gold data.

Fetch raw records from Open-Meteo Air Quality, Open-Meteo Weather, and OpenAQ
and write them to local Bronze Parquet partitions:

`python -m ingestion.build_bronze`

This command writes:
- `data/bronze/air_quality/`
- `data/bronze/weather/`
- `data/bronze/openaq_measurements/`

For a smoke test before fetching every OpenAQ location:

`python -m ingestion.build_bronze --max-openaq-locations 5`

## Local endpoints
- Airflow: http://localhost:8082
- Spark Master UI: http://localhost:8083
- Spark Worker UI: http://localhost:8081
- Streamlit: http://localhost:8501
- Kafka bootstrap (host): `localhost:9092`
- Kafka bootstrap (inside Compose network): `kafka:29092`
- MQTT: `localhost:1883`
- PostgreSQL: `localhost:5432`

## Important development notes
- Kafka is single-node KRaft for local development, not production high availability.
- All published Compose host ports bind to `127.0.0.1`; services are not exposed
  directly to the LAN. MQTT anonymous access is enabled only for local
  development. Add authentication/TLS before changing the MQTT bind address
  or exposing it beyond localhost.
- Spark is a small local standalone cluster. Tune worker memory/cores to the host.
- If Kafka has already expired offsets recorded in a Bronze checkpoint, Spark
  logs a data-loss warning and continues from available offsets so the stream
  does not remain stopped. Keep Kafka retention longer than expected outages
  when complete recovery is required.
- Airflow uses LocalExecutor and a shared PostgreSQL metadata database. Task
  execution requests use the API server's Compose-network URL, not `localhost`;
  all Airflow services load the same API JWT secret and web-session signing key
  from the ignored `.env.airflow-jwt` file. Airflow publishes records to Kafka;
  Spark writes Bronze, Silver, Quarantine, and Gold to the shared `./data`
  mount. Airflow and Spark share group ID 185 for compatible file access.
- The PostgreSQL init script runs only when the database volume is first created. If you change credentials after initialization, update database users manually or recreate volumes (which deletes local DB data).
- Do not commit `.env`, passwords, cloud keys, checkpoints, or real sensor secrets.
- Azure ADLS Gen2 replaces MinIO as the intended cloud object store. No MinIO service is included.
- This is a development scaffold; harden authentication, networking, TLS, backups, and monitoring before production use.


## CAPMAS governorate geocoding

OpenAQ aggregates with a missing or `Unknown` governorate are resolved in the PostgreSQL Gold loader using point-in-polygon against the 27 governorate-level CAPMAS boundaries in `data/reference/egypt_governorates_capmas.geojson`. The source, attribution, retrieval date, and EPSG:4326 coordinate reference are recorded in `data/reference/README.txt`. Existing non-Unknown source governorates and all non-OpenAQ sources are left unchanged. Coordinates outside exactly one governorate remain `Unknown`; no nearest-centroid or guessed assignment is used.

The initial correction classified 1,857 previously unknown OpenAQ aggregate rows as Cairo and one as Alexandria. The remaining unknown OpenAQ fact count is zero. Existing facts were merged into their corrected dimensional keys with weighted averages, min/max preservation, and measurement-count conservation; the warehouse still contains 83,583 fact rows and 83,586 represented measurements. The raw Silver Parquet records and streaming checkpoints were not rewritten. Future Gold batches are resolved by the loader before dimension/fact upsert.

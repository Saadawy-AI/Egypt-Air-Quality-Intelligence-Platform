# Airflow Platform Orchestration

**Updated:** 2026-11-25  
**Role:** Airflow is the platform control plane. Kafka and Spark remain the data plane.

## Runtime contract

Airflow coordinates scheduled and operational work. It does not replace Kafka, Spark Structured Streaming, the PostgreSQL loader, or Streamlit. Kafka transports events; the Spark Bronze/Silver/Gold streaming applications keep running under Docker Compose and retain their existing checkpoints. Airflow checks their outputs and coordinates acceptance gates. It must not start duplicate Spark streaming queries or reset checkpoints during ordinary DAG runs.

## DAG inventory

| DAG ID | Schedule (UTC) | Responsibility |
|---|---|---|
| `egypt_air_quality_platform` | Hourly at minute 15 | Main end-to-end control flow: preflight, API ingestion, fresh Bronze/Silver outputs, committed Gold batch, quality checks, PostgreSQL batch/row-count reconciliation, platform health. |
| `egypt_air_quality_monitoring` | Every 5 minutes | MQTT broker/bridge presence, Kafka required-topic availability, at least one live Spark worker plus the three active Bronze/Silver/Gold applications, PostgreSQL fact/batch availability and loader freshness. |
| `egypt_air_quality_ml` | Sunday 03:00 | Train PM2.5 candidate, evaluate against naive persistence baseline, register as qualified or rejected candidate. No automatic production promotion. |
| `egypt_air_quality_backup` | Daily 02:00 | Create PostgreSQL custom-format dumps for the application and Airflow metadata databases, archive active/candidate model artifacts, upload recent artifacts to R2, then verify remote bytes with SHA-256. |
| `egypt_air_quality_ingestion` | Manual only | Standalone ingestion utility. Scheduled ingestion belongs to the master platform DAG to avoid duplicate API publishing. |

All schedules are UTC. Airflow UI is published locally on port 8082.

## Main platform dependencies

```text
Preflight (Kafka + Spark + PostgreSQL)
  -> API ingestion to Kafka
  -> wait for fresh Bronze Parquet
  -> wait for fresh Silver Parquet
  -> wait for committed Gold batch (_SUCCESS + Parquet)
  -> Gold null/count quality gate
  -> PostgreSQL batch_id + row_count reconciliation
  -> platform health summary
```

The master DAG uses retries and task timeouts. Gold acceptance requires a committed, non-empty batch. The PostgreSQL gate checks the same Gold `batch_id` and compares the committed row count with the Gold Parquet row count.

## ML governance

Candidate artifacts are stored in the persistent Docker volume `ml_candidates`, mounted at `/opt/model-candidates`. The volume initializer assigns write access to the non-root Airflow UID. Production model files remain mounted read-only from `ml/models`.

The registry table is `ml_model_registry`. Candidate statuses are `qualified_candidate` and `rejected_candidate`. A rejected candidate is recorded as a normal evaluation outcome; it is not promoted or copied over the production model.

The first scheduled candidate evaluated on 2026-11-25 had MAE 8.667 versus naive persistence MAE 8.509. It failed the MAE baseline gate and was recorded as `rejected_candidate`; RMSE and R² improved, but that does not override the configured MAE gate.

## Backup scope and verification

The daily DAG creates:
- `airquality_*.dump`: application PostgreSQL database.
- `airflow_metadata_*.dump`: Airflow metadata database.
- `pm25_models_*.tar`: active and candidate model artifacts.

The R2 uploader filters to newly created artifacts, uploads them, checks object metadata/size, downloads each object to a temporary directory, and recomputes SHA-256. Existing Kafka KRaft and lake-checkpoint archives remain in the historical backup set, but the new daily DAG does **not yet** regenerate those two archives. Add a separate controlled checkpoint/Kafka backup procedure before describing the daily backup as a full data-lake recovery backup.

## Dashboard and API

The Streamlit application reads the latest run state for the four scheduled DAGs through Airflow 3's token endpoint and REST API. The panel is informational; task logs and retries remain in Airflow UI.

## Alerting configuration

Failure callbacks are implemented for the main platform, monitoring, ML, and backup DAGs. Push notifications require both `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` in the ignored project `.env` file. Neither key is currently configured, so callbacks log that push delivery is skipped. Airflow task state and the Streamlit status panel remain available.

After adding credentials, recreate the Airflow services so the environment is injected into the containers. Never commit `.env`.

## Operational validation performed

- Docker Compose configuration validation passed.
- Airflow API health endpoint reported healthy metadata DB, scheduler, triggerer, and DAG processor.
- DAG import-error check returned no import errors.
- Monitoring DAG passed Kafka, Spark, and PostgreSQL checks.
- Manual master platform run passed all eight tasks from preflight through PostgreSQL reconciliation and platform health.
- ML training and registry workflow succeeded; candidate was correctly rejected by the configured MAE baseline gate.
- R2 upload/download SHA-256 verification passed for the automated application-database/model backup and for the follow-up application-database + Airflow-metadata-database + model backup. The R2 bucket listing showed the new timestamped objects (20 objects total at validation time).

## Safety boundaries

- Do not run `docker compose down -v` as part of ordinary Airflow maintenance.
- Do not delete or reset Spark checkpoints from Airflow.
- Do not trigger the standalone ingestion DAG on a schedule.
- Do not automatically promote ML candidates; promotion requires a separate reviewed release step.
- Keep Airflow and PostgreSQL credentials in the ignored `.env` file only.

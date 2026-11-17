# Egypt Air Quality Intelligence Platform — Infrastructure

Local development infrastructure:
- PostgreSQL (two databases: `airflow`, `airquality`)
- Apache Kafka 4.x in single-node KRaft mode
- Eclipse Mosquitto MQTT broker
- Apache Spark standalone master + worker
- Apache Airflow 3.0.6 (LocalExecutor) for hourly ingestion
- Spark Bronze, validated Silver, and hourly Gold Parquet layers
- Streamlit dashboard backed by Gold aggregates

Azure is intentionally not emulated in Docker. The cloud data lake target is Azure Data Lake Storage Gen2; connect it in a later cloud-integration step. Local project files are mounted under `./data`.

## Prerequisites
Docker Desktop with WSL2 integration enabled, and Docker Compose v2.

## First-time setup (run from this directory in WSL Ubuntu)

1. Copy environment template:
   `cp .env.example .env`

2. Generate an Airflow Fernet key:
   `docker run --rm apache/airflow:3.0.6 python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`

3. Put the generated key in `.env` as `AIRFLOW_FERNET_KEY`. Replace every sample password with a strong unique password.

4. Make the init script executable:
   `chmod +x postgres/init/01-create-databases.sh`

5. Start infrastructure:
   `docker compose up -d --build`

6. Check services:
   `docker compose ps -a`

The Compose stack creates the Kafka topics, runs one initial ingestion pass,
and starts the Bronze, Silver, and Gold Spark streams. Airflow then runs
ingestion hourly; its DAG is available as `egypt_air_quality_ingestion` in
the Airflow UI. Set `OPENAQ_API_KEY` in `.env` to enable OpenAQ; Open-Meteo
ingestion does not require an API key. To run ingestion once manually, use:

`docker compose run --rm ingestion`

`docker compose logs -f airflow-scheduler spark-bronze spark-silver spark-gold`

The local lake is organized as:
- Bronze raw Kafka records: `./data/bronze`
- Validated Silver records: `./data/silver`
- Rejected records with validation reasons: `./data/quarantine`
- Hourly Gold aggregates: `./data/gold/hourly_measurements`
- Spark streaming state: `./data/checkpoints`

The dashboard reads Gold Parquet data directly. It refreshes its cached data
once per minute and can be filtered by source, governorate, and measurement.
The lookback window can be set with `DASHBOARD_LOOKBACK_DAYS` in `.env`.

## Local readiness checks before Azure

1. Run `docker compose config -q` to validate the Compose configuration.
2. Start the stack and confirm all services are healthy or running with
   `docker compose ps -a`.
3. Confirm the Airflow DAG succeeds and records are present in Kafka.
4. Confirm new Parquet files appear under Bronze and Silver, and that
   invalid records (if any) are written to Quarantine with a reason.
5. Confirm Gold hourly aggregates appear and the dashboard displays them.
6. Restart Spark services and confirm checkpoints resume processing without
   duplicating previously ingested observations.

Azure integration should replace the local lake paths with ADLS Gen2
locations and use managed identity or another approved secret-free workload
identity. Keep the same Bronze/Silver/Quarantine/Gold data contract.

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
- MQTT anonymous access is enabled only for local development. Add authentication/TLS before exposing it beyond localhost.
- Spark is a small local standalone cluster. Tune worker memory/cores to the host.
- Airflow uses LocalExecutor and a shared PostgreSQL metadata database.
- The PostgreSQL init script runs only when the database volume is first created. If you change credentials after initialization, update database users manually or recreate volumes (which deletes local DB data).
- Do not commit `.env`, passwords, cloud keys, checkpoints, or real sensor secrets.
- Azure ADLS Gen2 replaces MinIO as the intended cloud object store. No MinIO service is included.
- This is a development scaffold; harden authentication, networking, TLS, backups, and monitoring before production use.

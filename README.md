# Egypt Air Quality Intelligence Platform — Infrastructure

Local development infrastructure:
- PostgreSQL (two databases: `airflow`, `airquality`)
- Apache Kafka 4.x in single-node KRaft mode
- Eclipse Mosquitto MQTT broker
- Apache Spark standalone master + worker
- Apache Airflow 3.0.6 (LocalExecutor)
- Streamlit placeholder application

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
   `docker compose ps`

## Local endpoints
- Airflow: http://localhost:8082
- Spark Master UI: http://localhost:8080
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

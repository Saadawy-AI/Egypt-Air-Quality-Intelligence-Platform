FROM apache/airflow:3.0.6

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl gnupg \
    && install -d /usr/share/keyrings \
    && curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor -o /usr/share/keyrings/postgresql.gpg \
    && . /etc/os-release \
    && echo "deb [signed-by=/usr/share/keyrings/postgresql.gpg] https://apt.postgresql.org/pub/repos/apt ${VERSION_CODENAME}-pgdg main" > /etc/apt/sources.list.d/pgdg.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends postgresql-client-16 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY --chown=airflow:0 ingestion /opt/airflow/ingestion
COPY --chown=airflow:0 ml /opt/airflow/ml

RUN pip install --no-cache-dir \
    python-dotenv \
    kafka-python \
    pyarrow \
    pydantic \
    paho-mqtt \
    "psycopg[binary]==3.2.9" \
    boto3 \
    pandas==2.1.4 \
    scikit-learn==1.6.1 \
    joblib==1.4.2

ENV PYTHONPATH=/opt/airflow

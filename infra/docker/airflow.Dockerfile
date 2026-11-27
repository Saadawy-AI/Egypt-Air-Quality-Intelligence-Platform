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
# Bake Python import paths into the image; avoid Windows/WSL bind mounts.
COPY --chown=airflow:0 airflow/dags /opt/airflow/dags
COPY --chown=airflow:0 airflow/plugins /opt/airflow/plugins
COPY --chown=airflow:0 scripts /opt/airflow/scripts

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

# Retry only the Airflow 3.0.6 RTIF primary-key race; do not mask other DB errors.
COPY --chown=airflow:0 infra/docker/patches/patch_airflow_rtif.py /tmp/patch_airflow_rtif.py
RUN python /tmp/patch_airflow_rtif.py \
    && python -m py_compile /home/airflow/.local/lib/python3.12/site-packages/airflow/api_fastapi/execution_api/routes/task_instances.py \
    && rm -f /tmp/patch_airflow_rtif.py

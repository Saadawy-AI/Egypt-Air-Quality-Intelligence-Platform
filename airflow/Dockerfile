FROM apache/airflow:3.0.6

USER airflow

COPY --chown=airflow:0 ingestion /opt/airflow/ingestion

RUN pip install --no-cache-dir \
    python-dotenv \
    kafka-python \
    pyarrow

ENV PYTHONPATH=/opt/airflow

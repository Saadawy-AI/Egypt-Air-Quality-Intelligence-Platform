FROM python:3.12-slim

WORKDIR /app

COPY ingestion/mqtt/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY ingestion/__init__.py /app/ingestion/__init__.py
COPY ingestion/mqtt /app/ingestion/mqtt

ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "ingestion.mqtt.subscriber"]

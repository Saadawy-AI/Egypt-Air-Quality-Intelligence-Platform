FROM python:3.12-slim

WORKDIR /app

COPY ingestion/requirements.txt /app/requirements.txt
COPY ingestion/wheels /wheels
RUN pip install --no-index --find-links=/wheels -r /app/requirements.txt

COPY ingestion /app/ingestion

CMD ["python", "-m", "ingestion.main"]

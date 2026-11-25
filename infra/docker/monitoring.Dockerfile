FROM python:3.11-slim
WORKDIR /app
COPY monitoring/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY monitoring /app/monitoring
ENV PYTHONUNBUFFERED=1
CMD ["python", "-u", "-m", "monitoring.alerts.anomaly_detector"]

FROM python:3.11-slim
WORKDIR /app
COPY monitoring/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY monitoring /app/monitoring
ENV PYTHONUNBUFFERED=1
RUN useradd --system --uid 10001 --create-home --shell /usr/sbin/nologin app
ENV HOME=/home/app
USER 10001:10001
CMD ["python", "-u", "-m", "monitoring.alerts.anomaly_detector"]

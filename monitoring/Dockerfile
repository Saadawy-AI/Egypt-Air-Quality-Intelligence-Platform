FROM python:3.11-slim

WORKDIR /app
COPY monitoring/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY monitoring/anomaly_detector.py /app/anomaly_detector.py
COPY monitoring/anomaly_rules.py /app/anomaly_rules.py
ENV PYTHONUNBUFFERED=1
CMD ["python", "-u", "/app/anomaly_detector.py"]

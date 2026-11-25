FROM python:3.11-slim
WORKDIR /app
COPY postgres/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY postgres/loaders/gold_loader.py /app/gold_loader.py
COPY postgres/schemas/schema.sql /app/schema.sql
CMD ["python", "-u", "/app/gold_loader.py"]

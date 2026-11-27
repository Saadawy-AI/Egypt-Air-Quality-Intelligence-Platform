FROM python:3.11-slim
WORKDIR /app
COPY postgres/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY postgres/loaders/gold_loader.py /app/gold_loader.py
COPY postgres/schemas/schema.sql /app/schema.sql
RUN useradd --system --uid 10001 --create-home --shell /usr/sbin/nologin app
ENV HOME=/home/app
USER 10001:10001
CMD ["python", "-u", "/app/gold_loader.py"]

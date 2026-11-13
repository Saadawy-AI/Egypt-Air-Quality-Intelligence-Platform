#!/bin/bash
set -e
export PGPASSWORD="$POSTGRES_PASSWORD"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
  CREATE USER airflow WITH PASSWORD '${AIRFLOW_DB_PASSWORD:-change_me}';
  CREATE DATABASE airflow OWNER airflow;
  CREATE USER airquality WITH PASSWORD '${APP_DB_PASSWORD:-change_me}';
  CREATE DATABASE airquality OWNER airquality;
EOSQL

# Gold Dead-Letter Queue (DLQ) Replay Runbook

## Purpose

Gold stores a durable JSON record when a Silver batch notification cannot be processed. The Spark Gold stream can then advance its Kafka checkpoint without silently dropping the failed notification.

## Safe replay procedure

Run the commands from the Airflow scheduler container. The replay utility validates the Silver batch before it publishes anything.

1. List pending records:

   ```bash
   docker compose exec airflow-scheduler sh -lc 'find /opt/project-data/gold/dead_letter -maxdepth 1 -type f -name "*.json" -printf "%f\\n"'
   ```

2. Validate one record without publishing:

   ```bash
   docker compose exec airflow-scheduler python /opt/airflow/scripts/replay_gold_dlq.py \
     --file /opt/project-data/gold/dead_letter/<record>.json
   ```

3. Repair/rebuild the Silver batch if validation reports a missing commit, invalid path, or row-count mismatch. Do not edit the DLQ JSON to conceal a mismatch.

4. Publish only after the dry-run succeeds:

   ```bash
   docker compose exec airflow-scheduler python /opt/airflow/scripts/replay_gold_dlq.py \
     --file /opt/project-data/gold/dead_letter/<record>.json --publish
   ```

The utility checks the Silver dataset identity, canonical batch path, `_SUCCESS` marker, and exact Parquet row count. It sends the original notification to `silver-batch-notifications` using SASL/PLAIN over TLS. If the project-data mount is writable, it archives the source under `dead_letter/replayed/` after Kafka acknowledges the publish. If Airflow has a read-only mount, the utility reports `archive_required: true`; then archive the file from the project host after confirming the broker acknowledgement:

   ```bash
   mkdir -p data/gold/dead_letter/replayed
   mv data/gold/dead_letter/<record>.json data/gold/dead_letter/replayed/
   ```

A repeated failure is written as a new DLQ record by the Gold stream.

## Current state (2026-11-27)

All three previously pending DLQ records have been replayed after validating or repairing their Silver batches. The pending top-level DLQ directory is empty; the original JSON records are retained under `dead_letter/replayed/`.

Silver batch 158 was rebuilt from its immutable Bronze source files. The Bronze source log contained 20 Parquet files and 60 records; all 60 passed the Silver validation logic. The previous Silver output contained only 38 rows. The previous output was retained under `data/silver/archive/`, and the repaired batch now contains 60 rows with a valid `_SUCCESS` marker. Its original notification was then replayed without altering its declared row count.

The Silver writer now reads back the committed Parquet output and compares the persisted row count (and quarantine count when present) with the micro-batch counts **before** publishing a Gold notification. A mismatch fails the micro-batch rather than sending a misleading notification.

## Operational notes

- Replay one record at a time and retain the archived JSON for audit.
- Never reset or delete the Gold streaming checkpoint to replay a record.
- Do not publish a notification with a manually altered `silver_path` or `row_count`.
- After replay, verify the new Gold batch and PostgreSQL load, then confirm the DLQ monitoring task is clear.

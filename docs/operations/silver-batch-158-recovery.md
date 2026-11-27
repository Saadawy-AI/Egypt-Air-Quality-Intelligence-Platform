# Silver Batch 158 Recovery Record

Date: 2026-11-27

## Incident

Gold DLQ record `00000000000000000090_air_quality_158.json` reported 60 rows for Silver batch 158, while the committed Silver Parquet output contained 38 rows. The original notification was not altered or replayed until the source data had been audited.

## Source audit

- Silver checkpoint source log: `data/checkpoints/silver/air_quality/sources/0/155`.
- The source log referenced 20 immutable Bronze Parquet files.
- Each Bronze file contained 3 records: 60 source records total.
- Re-running the project's `parse_bronze_records` validation logic against those files confirmed 60 parsed records, 60 valid records, 0 invalid records, and 60 distinct record IDs.
- The original Silver batch contained 38 rows, so 22 validated records were missing from its persisted output.

## Recovery

- Rebuilt the Silver batch from the original Bronze files using the existing Silver parser and validation logic.
- Wrote to a staging directory and verified all 60 rows plus the `_SUCCESS` marker before replacing the active batch.
- Preserved the original 38-row output at:
  `data/silver/archive/air_quality_batch_id_00000000000000000158_pre_repair_20261127T020405Z/`.
- Replaced the active Silver batch 158 with the verified 60-row output.
- Dry-run validated the original DLQ notification, then published it unchanged to `silver-batch-notifications`.
- Gold generated batch `1000000158` with 15 aggregate rows; PostgreSQL recorded `(1000000158, 15)`.
- Archived the original DLQ JSON under `data/gold/dead_letter/replayed/`.

## Preventive change

`spark/silver/ingestion.py` now checks the persisted Silver Parquet row count and `_SUCCESS` marker against the cached micro-batch count before publishing a notification. It performs the equivalent check for quarantine output. A mismatch raises an error and prevents an inaccurate notification from reaching Gold.

## Final verification

- Silver batch 158: 60 rows, `_SUCCESS` present.
- Gold batch 1000000158: 15 rows, `_SUCCESS` present.
- PostgreSQL processed-batch marker: `(1000000158, 15)`.
- Gold DLQ: 0 pending records; 3 original records retained in `replayed/`.
- Spark Bronze, Silver, and Gold applications: RUNNING.
- Full local test suite: 164 passed, 218 warnings, 3 subtests passed.

Do not delete the archived pre-repair Silver batch until the project's normal data-retention review.

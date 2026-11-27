# Bronze Spark Checkpoint Migration

## Result

Bronze checkpoint state is now stored under `/opt/project-data/checkpoints/bronze/<dataset>`, matching the explicit Bronze layer layout.

## Migration performed

- Gracefully stopped only the `spark-bronze` service before copying checkpoint state.
- Preserved the previous live checkpoints at `data/checkpoints/<dataset>` as rollback copies.
- Moved the pre-existing, stale canonical directories under `data/checkpoints/bronze/<dataset>` into:
  `data/checkpoints/archive/bronze-canonical-pre-migration-20261127T015007Z/`.
- Copied the live checkpoint directories for `air_quality`, `openaq_measurements`, and `weather` into their canonical Bronze locations.
- Compared each source and destination recursively; all three copies matched.
- Updated `spark/bronze/ingestion.py` to use `/opt/project-data/checkpoints/bronze` and restarted Bronze.

## Verification

- Bronze, Silver, and Gold Spark applications were all reported RUNNING after the restart.
- The Kafka TLS listener remained healthy and the Spark applications had their truststore mounted.
- The original legacy checkpoint directories were retained; do not delete them until a separate retention review.

## Rollback

If Bronze cannot resume, stop only `spark-bronze`, restore the relevant dataset directory from the archived canonical snapshot if needed, and revert the checkpoint base path to `/opt/project-data/checkpoints`. Keep both the original legacy checkpoint directories and the migration archive intact until recovery is verified.

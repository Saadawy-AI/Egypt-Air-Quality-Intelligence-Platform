# Operations Recovery, Backup and Streaming Observability Runbook
Date: 2026-11-24
Status: procedure/design; live destructive recovery drills are NOT yet completed.

## Verified storage map
All Spark layers/checkpoints are bind-mounted from project data/ to /opt/project-data.
- Bronze: data/bronze/<dataset>/ (Parquet + _spark_metadata)
- Silver: data/silver/<dataset>/ (batch_id-partitioned Parquet)
- Gold: data/gold/hourly_measurements_v4/ (batch_id/window_date partitions)
- Spark state: data/checkpoints/ (offsets, commits, sources, state-store)
- Kafka KRaft volume: egypt-air-quality_kafka_data -> /var/lib/kafka/data
- Mosquitto volumes: egypt-air-quality_mosquitto_data and egypt-air-quality_mosquitto_log
- PostgreSQL volume: egypt-air-quality_postgres_data; logical backups and temporary-DB restore drill were previously verified.

Never restore a Spark checkpoint independently of its query/data. Silver checkpoints contain stateful deduplication state. Never copy a live checkpoint tree.

## Backup policy
| Component | Method | Frequency | Consistency |
|---|---|---|---|
| Bronze + _spark_metadata | filesystem archive | daily | stop Bronze writer; preserve data+metadata |
| Silver | filesystem archive | daily | stop Silver writer; preserve complete batches/_SUCCESS |
| Gold | filesystem archive | daily | stop Gold writer; preserve complete batches/_SUCCESS |
| Spark checkpoints | filesystem archive | daily | stop owning query; capture entire tree |
| Kafka KRaft | cold volume archive | daily/before changes | stop broker cleanly |
| Mosquitto | cold volume archive | daily | stop broker cleanly |
| PostgreSQL | pg_dump custom format | daily | existing verified workflow |
| Secrets | encrypted secret manager | on change | never plaintext in Git or normal archive |
Retention target: 7 daily, 4 weekly, 3 monthly; at least one encrypted off-host copy. Verify SHA-256 and perform monthly restore drills. Local-only backups do not protect against host/disk loss.

## Data lake + checkpoint backup sequence
Maintenance window only; not an unattended job.
1. Record PostgreSQL Gold batch/row counts and Kafka topic end offsets.
2. Gracefully stop writers: docker compose stop -t 120 spark-bronze spark-silver spark-gold
3. Confirm all three stopped and driver processes exited.
4. Archive complete data/bronze, data/silver, data/gold and data/checkpoints into timestamped backups/. Preserve hidden files, permissions, _SUCCESS and _spark_metadata; do not filter checkpoint files.
5. Generate SHA-256 manifest and verify archive listing.
6. Restart writers: docker compose start spark-bronze spark-silver spark-gold.
7. Verify streams resume, new batches flow and PostgreSQL counts progress.
8. Copy archive+manifest to encrypted off-host storage.
If stop fails, do not archive a live checkpoint.

## Kafka cold backup + isolated restore
Kafka is a single KRaft broker/controller with RF=1: local/dev only, not HA.
Cold backup:
1. Capture topic metadata/configs and per-partition end offsets.
2. docker compose stop -t 120 kafka
3. Archive the entire named volume egypt-air-quality_kafka_data using a helper container with read-only source volume and writable host backup directory.
4. Verify archive listing and SHA-256.
5. Restart Kafka; verify broker API, topic metadata and offsets.
Restore drill (never overwrite active volume):
1. Create a separate temporary Docker volume.
2. Extract archive preserving ownership/modes.
3. Start isolated Kafka using exact image apache/kafka:4.1.0, matching KRaft node ID, cluster metadata, listeners and log directory, separate network/ports.
4. Verify broker startup, topic/partition metadata and readable records at captured offsets with bounded read-only consumers.
5. Remove only temporary drill resources after evidence is recorded.
6. Never run kafka-storage format on restored data; it creates new metadata and can destroy recovery. Logical topic export is not a substitute for KRaft cold backup.

## Recovery and idempotency acceptance
- Bronze: restore Parquet + matching _spark_metadata; compare inventory, row counts, partition dates; replay must not create duplicate committed output.
- Silver: restore Parquet + matching checkpoint as a pair; in isolated copy replay a known duplicate; verify one logical record per record_id and correct quarantine counts.
- Gold: replay a known Silver notification twice in isolation; stable dataset/batch identity must yield one Gold batch/logical output. Compare processed_gold_batches and fact table.
- Kafka: isolated restore; compare configs, partitions and records at captured offsets.
- E2E reconciliation: source accepted = Bronze committed; Silver valid + quarantined = input; Gold rows = corresponding PostgreSQL loaded rows. Document intentional filtering/deduplication.
Never run destructive replay against live stack; use isolated copies and temporary DBs.

## Consumer lag and Spark Streaming observability
Kafka commands:
- kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
- kafka-consumer-groups.sh --bootstrap-server localhost:9092 --all-groups --describe
- kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic <topic> --time -1
Spark Structured Streaming direct Kafka sources may not register normal consumer groups. Empty group list does NOT prove zero lag. Capture StreamingQueryProgress JSON: queryId/runId, batchId, timestamp, numInputRows, inputRowsPerSecond, processedRowsPerSecond, durationMs, source startOffset/endOffset, stateOperators. Compute per-partition backlog from Kafka end offsets minus Spark committed source offsets. Alert on growing backlog, stalled batchId, repeated failures, processing rate below input rate and state-store growth. Persistent lag telemetry/alerts are not yet verified; do not report zero lag without offset comparison.

## Resource and disk controls
Latest observed snapshot (2026-11-24): WSL memory 5.8 GiB, 1.3 GiB available, swap 3.6/4.0 GiB used. D: 301 GiB total, 270 GiB used, 31 GiB free (90%). Docker: 15.53 GiB images, 5.133 GiB containers, 4.143 GiB volumes, 2.782 GiB build cache. No volumes/cache were pruned.
- Keep Spark concurrency unchanged until measured memory budget and soak test.
- Measure per-container memory at normal/peak ingestion; tune Spark driver/executor/JVM and Docker limits together, leaving host/WSL headroom. Arbitrary caps risk OOM.
- Relocate Docker/WSL virtual disk or datasets to larger SSD before adding bulk data/Azure tooling.
- Identify anonymous volume labels and mount consumers before deletion. Never routine docker system/volume prune.
- Disk alert 80%, critical 90%; reserve space for compaction/checkpoints/backups.
- Current 31 GiB free is constrained; check archive size before duplicating datasets.

## Security and Git
- Keep .env and JWT keys out of Git, images, logs and ordinary backup archives.
- Rotate credentials before public/shared push; history may retain secrets even if current .env is ignored.
- Before release run git status --short, git ls-files, git check-ignore -v .env .env.airflow-jwt and a secret scanner (e.g. gitleaks).
- Never git clean/reset or delete untracked/modified files as cleanup.
- Kafka currently uses PLAINTEXT and RF=1: trusted local development only. Production/Azure needs TLS/SASL, ACLs, secret management, private networking and HA.
- Never put secret values in reports/chat.

## Acceptance status
Previously verified: 153 Python tests passed; Compose config valid; PostgreSQL logical backup/restore validated in temporary DBs. Latest runtime observation: all 16 services Up.
Not completed: cold Kafka backup+isolated restore; consistent lake/checkpoint archive+restore; controlled idempotency/replay; durable Spark progress/lag telemetry; secret-history scan; resource tuning/soak test; final fault-injection E2E.
Azure readiness remains NOT APPROVED pending evidence for all gates.

## Live read-only verification addendum (2026-11-24)
- All 16 Compose services were Up at inspection; every container showed restart count 0 and OOMKilled=false.
- Kafka topics present: air-quality, weather, openaq-measurements, silver-batch-notifications, plus internal __consumer_offsets. Each application topic has partitions 0-2.
- Captured end offsets: air-quality 11770/13024/12453; weather 12863/12676/12380; openaq-measurements 87/141/125; silver-batch-notifications 2/2/2 (partition order 0/1/2). These are broker end offsets only, not lag measurements.
- kafka-consumer-groups --list returned no group names. This is consistent with Spark direct Kafka source; it does not establish zero lag.
- Current git index lists .env.example but not .env. .env and .env.airflow-jwt are ignored by .gitignore. This does not prove historical commits are free of secrets. No gitleaks binary was found on PATH, so a verified secret-history scan remains outstanding.
- Git working tree remains highly modified (26 modified, 2 deleted status entries, 31 untracked, and 88 deleted paths in the displayed status summary). No cleanup/reset/prune was performed.
- Layer sizes from the initial filesystem walk: Bronze 12 MiB, Silver 14 MiB, Gold 4.6 MiB. Checkpoint size walk did not complete in the observation window; no full lake archive was attempted.
- End-to-end pipeline success is supported by the previously recorded batch 2000000045 processing evidence and 153 passing tests, but a fresh fault-injection E2E/replay drill was deliberately not run against the live stack.

## Host disk cleanup record (2026-11-24)
User authorized removal of safe/reclaimable items. Executed:
- Docker builder cache prune: removed all 2.782 GiB reclaimable build cache (no volumes/containers touched).
- Docker image prune -a: removed unused images; Docker reported 3.073 GB total reclaimed. This included unused Kubernetes/Kind support images and an unused ML trainer image; these can be pulled/rebuilt if those workloads are needed again.
- WSL pip cache purge: reduced pip cache from 213.4 MB HTTP cache + 78 KB wheels to 0 bytes HTTP cache + 328 bytes wheel directory.
- Inspected User Temp and Windows Temp for files older than 30 days, excluding swap.vhdx; no eligible files were returned. No Temp files, Chrome model, WSA files, VHDX files, Docker volumes or project data were deleted.
Post-cleanup verification: all 16 project Compose services remained Up; restart counts 0 and OOMKilled=false. Core Kafka, PostgreSQL and Mosquitto volumes still exist. Docker now reports Images 12.45 GB (14 active/15 total), Containers 5.135 GB, Volumes 4.143 GB, Build Cache 0.
Important: Windows C: still reports approximately 8.6 GB free / 96% used. Docker/WSL storage is in dynamically expanding VHDX files; freeing blocks inside the virtual disks does not automatically shrink the host VHDX. Offline VHDX compaction would require a planned maintenance window, cleanly stopping Docker/WSL, and verifying exact disk paths and available Windows tooling. Not performed in this cleanup to avoid interrupting the live ingestion stack.


## Current recovery/security acceptance update — 2026-11-24 19:55 UTC

Supersedes older open items in the sections above where newer evidence exists:
- Cloudflare R2 off-host copies are active and verified (11 objects; size/SHA-256 metadata matched).
- PostgreSQL restore drills from R2 passed in isolated temporary databases; the temporary databases were removed afterward.
- Kafka KRaft and lake/checkpoint cold archives have separate isolated restore validation and dated manifests.
- OpenAQ key rotation and Airflow Fernet rotation are complete. Airflow services use the current key fingerprints; OpenAQ API smoke request returned HTTP 200.
- GitHub history cleanup is complete on `main`, `fix/kafka-persistence`, and `master`; reachable private env paths are zero and historical secret-like assignments in `.env.example` are blank. See `docs/GIT_HISTORY_CLEANUP_2026-11-24.md`.

Still open:
- WSL/host memory headroom and sustained soak test; do not perform a WSL shutdown while the Docker stack is active without a planned maintenance window.
- Fault-injection and replay acceptance across the live pipeline.
- Real ESP32/MQTT sensor payload receipt (broker/bridge connection alone is not a payload acceptance test).
- Telegram delivery configuration and end-to-end notification test (optional if Telegram is not a requirement).
- Production Kafka TLS/SASL, ACLs, multi-broker RF=3/min ISR=2 and Azure private networking/identity.
- Final Azure readiness approval remains withheld.

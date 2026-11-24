# Local Infrastructure Readiness Audit — 2026-11-24

## Scope and safety
Read-only audit of the live Docker Compose stack, Kafka metadata, Spark logs, Airflow run history, PostgreSQL aggregates, repository state, and selected configuration/source files. No container was restarted, no volume/data was deleted, no checkpoint was reset, and no credentials were printed. Streamlit dashboard work remains deferred.

## Verified healthy
- Docker Compose configuration validation passed.
- All 16 expected project containers were Up at audit time.
- Kafka, PostgreSQL, Spark master, and Spark worker reported healthy.
- Container restart count was 0 for PostgreSQL, Kafka, MQTT-to-Kafka bridge, Spark Bronze/Silver/Gold, and PostgreSQL loader at inspection time; OOMKilled=false.
- Kafka topics exist: air-quality, weather, openaq-measurements, silver-batch-notifications. Each has 3 partitions and a single replica; all partitions had a leader and ISR.
- Kafka offsets were non-zero for all ingestion topics. MQTT-to-Kafka bridge logs confirmed connection to Kafka and MQTT subscription.
- Spark Bronze logs confirmed all 3 streams started. Silver logs showed processing a non-empty OpenAQ batch (5 valid, 0 quarantined) and publishing a notification. Gold logs confirmed creation of corresponding Gold batch 2000000045.
- PostgreSQL currently contains 92,730 fact rows and 20 processed Gold batches; ledger row_count sums to 92,730. The fact table check found 0 invalid/null aggregate values and no non-positive measurement_count.
- Latest Airflow scheduled runs on 2026-11-24 00:00–05:00 UTC were successful. Two older failed runs remain in history (2026-11-17 and 2026-11-18); later scheduled runs shown were successful.
- The PostgreSQL loader's latest logs showed successful load of Gold batch 2000000045 (5 rows).

## Findings requiring work before Azure
1. **Single-broker Kafka is local-development grade only.** Replication factor is 1 for application topics and offsets; this is not broker fault tolerant. Plan a multi-broker/Event Hubs target and appropriate replication/retention/DLQ/schema governance for production.
2. **No per-service CPU/memory reservations or limits are defined in the inspected Compose file.** Docker stats showed Spark worker using about 926 MiB, Kafka about 506 MiB, Spark Silver about 447 MiB, Gold about 374 MiB, and Bronze about 332 MiB at the snapshot. Host free memory was not successfully retrieved in this shell session, so the earlier memory percentage must not be treated as current.
3. **Loader has historical memory/connectivity errors in retained logs.** The current loader is running with restart count 0 and its recent log tail shows successful processing, but older log entries show PostgreSQL connection timeouts/startup and an OSError: Cannot allocate memory during batch discovery. The source now uses os.scandir to avoid materializing large Parquet globs. Keep observing under load; do not claim the historical issue is permanently closed without a sustained soak test.
4. **Automated tests were not completed.** pytest is not installed in the Airflow and PostgreSQL runtime containers. The WSL-host test invocation did not return output during this audit. No passing test-suite result is claimed.
5. **Backup/restore is unverified.** Persistent PostgreSQL/Kafka/Mosquitto volumes exist, but a tested backup, off-host copy, restore drill, and RPO/RTO evidence were not established.
6. **End-to-end failure/recovery testing is still outstanding.** No controlled broker outage, duplicate replay, checkpoint recovery, backpressure, or data-loss simulation was performed because that would disrupt live streaming/checkpoints.
7. **Monitoring is partial.** The anomaly detector is running, but production-grade metrics, alerting, log retention/centralization, consumer-lag dashboards, and service SLOs still need verification.
8. **Repository working tree contains many modified, deleted, and untracked files.** No reset, clean, commit, or destructive cleanup was performed. Review the existing staged .env deletion and all outstanding changes deliberately before committing. .env and .env.airflow-jwt are ignored by Git; secret contents were not read or emitted.
9. **Security hardening remains incomplete for production.** Local published service ports are loopback-bound, and PostgreSQL has a private Compose network; Kafka/MQTT are plaintext within the local network. Azure migration must use managed identity/Key Vault, TLS, least privilege, network controls, and secret rotation.

## Current disposition
**Local data path is operating and producing Gold batches; Azure readiness is NOT YET APPROVED.** The live stack was left running and untouched after the read-only audit.

## Next execution gates
- Establish a reproducible test runner (host/CI environment with pinned development dependencies) and run all ingestion, Spark, PostgreSQL, and integration tests.
- Run a sustained end-to-end soak test and controlled recovery tests with explicit checkpoint/volume backups.
- Implement and test automated PostgreSQL backup/restore and data retention.
- Add resource budgets, health checks for remaining services, structured log retention, metrics, and alerting.
- Resolve all Git changes safely; keep secrets out of version control and validate a clean clone can be configured from .env.example.
- Only then sign off Infrastructure, Ingestion, Kafka, Spark, and end-to-end acceptance and begin Azure migration.


## Follow-up validation — 2026-11-24
- Added a root `pytest.ini` with explicit test paths and repository-root import path. This intentionally excludes the standalone root-level connectivity probe from the offline automated suite.
- Corrected package imports for the monitoring and Streamlit data-helper tests by adding package `__init__.py` files and using package-qualified imports. No running service image or container was rebuilt/restarted for these test-only changes.
- Full project automated suite now passes in the WSL project virtual environment: **153 passed, 218 warnings, 3 subtests passed in 27.11s**. Warnings are kafka-python/importlib-resources deprecations; no test failures remain in the configured suite.
- Docker Compose configuration validation still passes after the test configuration changes.
- Current Docker stats snapshot: Spark worker ~801 MiB, Kafka ~539 MiB, Spark Silver ~438 MiB, Bronze ~329 MiB, Gold ~353 MiB; all 16 containers remained Up. These are point-in-time readings, not limits or guarantees.
- WSL memory snapshot: 5.8 GiB total, ~1.4 GiB available, ~3.5 GiB swap used. Project disk (D:) is 90% used with ~31 GiB free. This is a capacity risk; avoid increasing parallel Spark workloads until the host/WSL memory and disk budget is addressed.
- Resource limits are still not configured in Compose. They were deliberately not added blindly because the current Spark workers and streams are already memory intensive and a too-low cap could terminate live streaming tasks.
- Git review confirms `.env` and `.env.airflow-jwt` are ignored by `.gitignore` and are not currently tracked in the index; `.env` appears as a staged deletion from the repository index. Secret values were not inspected. Do not stage or commit local environment files. Historical repository commits may still contain previously tracked secrets and require credential rotation before any public/shared push.
- The working tree still contains substantial pre-existing staged, modified, deleted, and untracked project changes. No cleanup/reset/commit was performed.

## Updated disposition
**Automated unit suite: PASS (153 tests). Compose syntax: PASS. Live stack: all 16 containers Up. Azure readiness: NOT APPROVED** until production Kafka resilience, resource/capacity planning, backup/restore, failure-recovery tests, monitoring/alerting, and Git/security review are completed.


## PostgreSQL backup and restore drill — 2026-11-24
- Created custom-format PostgreSQL backups under `backups/2026-11-24/` for both `airquality` (955,318 bytes) and `airflow` (165,797 bytes).
- Both archives passed `pg_restore --list` validation.
- Performed a real restore drill into separate temporary databases. Restored `airquality` contained 92,730 `fact_hourly_measurements` rows and 20 `processed_gold_batches`; restored `airflow` contained 41 DAG runs and 41 task instances.
- Dropped only the temporary validation databases after the counts matched; production databases were not modified.
- SHA-256 hashes and a restore runbook are recorded in `backups/2026-11-24/BACKUP_MANIFEST.md` and `POSTGRES_RESTORE_RUNBOOK.md`.
- `/backups/` is now excluded from Git and Docker build contexts. The backup remains local to this machine; an encrypted off-host copy is still required.
- This does not yet protect Kafka data/offsets, Mosquitto persistence, Spark checkpoints, Bronze/Silver/Gold Parquet data, model artifacts, or PostgreSQL globals/roles.


## Post-backup runtime recheck — 2026-11-24
- Airflow `dags list-import-errors` returned `No data found` (no current DAG import errors).
- The latest 6 scheduled hourly ingestion runs (00:00–05:00 UTC on 2026-11-24) were all `success`. The retained history still includes two older failed runs on 2026-11-17 and 2026-11-18; they remain historical records and were not deleted.
- PostgreSQL still reports latest Gold batch `2000000045`, processed at `2026-11-24 05:14:08 UTC`, with 92,730 rows represented in the batch ledger.
- Final Compose recheck showed all 16 long-running project services `Up`; Kafka, PostgreSQL, Spark master, and Spark worker were healthy. Kafka init and Airflow init are one-shot jobs and are expected to be exited successfully.
- Loader logs still contain an older `Cannot allocate memory` stack trace from 02:21 UTC, before the current lazy `os.scandir` implementation was in place. The current source uses lazy directory traversal, and subsequent loader logs show successful Gold loads. Continue monitoring; the historical stack trace is not evidence of a current crash.


## Audit note correction
The initial test-status paragraph above records the state before the WSL virtual environment was located. The later follow-up supersedes it: the configured full suite completed successfully with 153 passed. `pytest` remains absent from the Airflow and PostgreSQL runtime containers; test execution is performed from the project's WSL `.venv`.


## Live pipeline acceptance recheck — 2026-11-24 15:03 UTC
- Re-ran the complete configured project test suite from host Python after installing the two missing test-collection dependencies (`paho-mqtt`, `psycopg[binary]`): **153 passed in 11.65s**, exit code 0.
- `docker compose config -q` passed; `git diff --check` reported only Windows LF→CRLF notices, no whitespace errors.
- All 16 project containers were Up at the latest status check. Docker stats showed Spark worker about 927 MiB, Silver 371 MiB, Bronze 341 MiB, Gold 148 MiB, Kafka 353 MiB, PostgreSQL 71 MiB. These are snapshots, not resource limits.
- Disk free at latest host check: C: about 38.1 GB; D: about 45.7 GB.
- Verified refreshed Airflow manual run `manual__openaq_key_refresh_20261124b` succeeded. OpenAQ 7 rows and Open-Meteo 2,592 rows were published to Bronze.
- Verified downstream propagation: Silver OpenAQ batch 59 retained 5 new records after deduplication; the Air-quality batch 53 and Weather batch 52 were empty after deduplication of repeated Open-Meteo observations. Gold batch 2000000059 committed 5 rows and the PostgreSQL loader loaded it successfully.
- PostgreSQL increased to 92,760 facts and 26 processed Gold batches; batch 2000000059 is in the ledger with row_count=5. Integrity query found zero invalid fact rows.
- WSL Ubuntu remains Running in WSL2 but its shell still has not been recovered. A full WSL shutdown remains deferred because it could interrupt Docker and the live stack.

## Updated disposition
**Local ingestion-to-PostgreSQL path: PASS for the refreshed run. Automated tests: PASS (153). Compose validation: PASS. Azure readiness: NOT APPROVED** pending WSL recovery, sustained soak/recovery tests, off-host backup coverage for lake/checkpoints/Kafka/MQTT, production monitoring/resource controls, Kafka high availability/security hardening, and deliberate Git/history/security review. Streamlit dashboard work remains deferred.


## WSL responsiveness recheck — 2026-11-24 15:04 UTC
- Ubuntu WSL2 responded successfully to a lightweight Bash command (`echo`, `free -h`, `df -h`) and a separate `ps` process listing. Basic shell execution is therefore working again.
- A heavier combined diagnostic command timed out with `Wsl/Service/WSAETIMEDOUT`, so recovery is **partial/intermittent**, not yet a sustained pass.
- WSL snapshot: 5.8 GiB RAM total, 4.5 GiB used, 1.3 GiB available; 4 GiB swap essentially full. Memory PSI showed recent pressure (`some avg10=0.21`, `full avg10=0.14`). Host Windows RAM: 15.79 GiB total, 1.92 GiB available (87.8% used); host pagefile: 11.02 GiB total, 3.07 GiB free.
- Current `.wslconfig` is 6 GB memory / 4 GB swap with `autoMemoryReclaim=dropcache`; no WSL configuration changes were made. Increasing WSL memory is not advisable while host free RAM is below 2 GB.
- Docker Compose recheck after WSL returned: all 16 project containers remain Up; PostgreSQL, Kafka, Spark master and Spark worker are healthy. No container restart or shutdown was performed.


## Off-host R2 backup inventory verification — 2026-11-24
- Used the configured R2 S3-compatible endpoint without printing credentials to list the bucket prefix `egypt-air-quality/backups/2026-11-24/`.
- Confirmed **11 objects** in the R2 bucket `egypt-air-quality`, including the PostgreSQL dumps, Kafka KRaft archive, lake/checkpoint archive, manifests, and runbook.
- HEAD metadata checks for `airquality.dump`, `airflow.dump`, `kafka-kraft-20261124.tar`, and `lake-checkpoints-20261124.tar` all passed: remote ContentLength matches the local artifact and the stored `sha256` metadata matches a freshly calculated local SHA-256.
- Local archive sizes and hashes were rechecked against the manifests; all four artifacts matched exactly.
- PostgreSQL R2 download/restore drill remains PASS. Kafka isolated KRaft restore and lake/checkpoint archive compare/extract validation are documented in their manifests.


## Git secret-history review — 2026-11-24
- Confirmed the local `.env` file still exists and is ignored by `.gitignore`; its deletion remains staged in Git, and it was not read or printed during this review.
- `git log --all -- .env` confirms `.env` existed in historical commits. The retained redacted Gitleaks report flags historical `OPENAQ_API_KEY` and `AIRFLOW_FERNET_KEY` entries in `.env`, plus a false-positive placeholder in `.env.example`. Secret values were redacted and never displayed.
- Updated the current `.env.example` Fernet setting to an empty value with a local-generation comment, eliminating the misleading high-entropy placeholder. The historical commits still contain the earlier sample and possible real credentials.
- Do not push/share this Git history until the OpenAQ key and Airflow Fernet/related credentials have been rotated as appropriate and the historical secret-bearing commits have been purged using a reviewed history-rewrite plan. No commit, push, reset, or history rewrite was performed.

- Extended R2 PostgreSQL restore drill to the Airflow database: downloaded `airflow.dump` from R2, verified size/SHA-256, validated 411-entry custom archive, restored into isolated `airflow_r2_restore_drill_20261124`, verified 41 DAG runs and 41 task instances, then dropped the temporary DB and confirmed it was absent. Both PostgreSQL databases now have successful R2 download-and-restore validation.


## Scheduled-ingestion soak recheck — 2026-11-24 15:15 UTC
- The next scheduled Airflow run, `scheduled__2026-11-24T15:00:00+00:00`, completed successfully; its `ingest_sources` task was `success`. `airflow dags list-import-errors` returned `No data found`.
- The 15:00 UTC run persisted another Bronze set: OpenAQ 7 rows, Open-Meteo air quality 1,296 rows, and Open-Meteo weather 1,296 rows.
- Silver's 15:10 UTC trigger committed batches OpenAQ 60, air_quality 54, and weather 53. All three were empty after deduplication because this scheduled pull repeated observations already processed from the 14:50 UTC run. No duplicate Gold batch was produced; PostgreSQL correctly remained at 92,760 facts and 26 processed Gold batches.
- MQTT bridge logs confirm successful Kafka broker connection, MQTT broker connection, and subscription to `egypt/air-quality/#`. No actual sensor payloads were observed during the check; no synthetic sensor data was injected into production topics.
- Docker inspection: MQTT-Kafka bridge, Silver, Gold, PostgreSQL loader all `running`, restart_count=0, OOMKilled=false. Airflow scheduler restart_count=1 from the deliberate service recreation for the OpenAQ key refresh; OOMKilled=false.


## Airflow Fernet-key remediation — 2026-11-24 15:18 UTC
- Live Airflow metadata DB had 0 rows in `connection`, 0 rows in `variable`, and 0 rows in `xcom`; no encrypted connection/variable payloads needed migration.
- Generated a new Fernet key locally and replaced only `AIRFLOW_FERNET_KEY` in the ignored local `.env`; the key value was never printed or written to reports.
- Recreated only the four Airflow runtime services (API server, scheduler, DAG processor, triggerer), without recreating dependencies or touching PostgreSQL/Kafka/Spark.
- Verified the SHA-256 fingerprint of `AIRFLOW__CORE__FERNET_KEY` matches across all four containers and the host `.env` (`5d033ae87bc4`).
- Airflow API health endpoint reports metadatabase, scheduler, triggerer, and DAG processor all `healthy`; DAG import-error listing returned `No data found`.
- The historical Fernet key is therefore invalidated for the current deployment. Historical Git commits still contain the old key text and still need history cleanup before publishing/sharing.
- The historical OpenAQ API key finding remains unresolved: rotate/revoke it in the OpenAQ account, then update `.env` and recreate Airflow services. This cannot be completed safely without issuing a new key through the account owner.


## Monitoring / anomaly-detector acceptance — 2026-11-24 15:20 UTC
- Anomaly detector completed its 2-minute polling cycles without runtime errors and produced three warning alerts at 15:01:54 UTC: sulphur_dioxide in Beheira, dust in Suez, and pm10 in Suez.
- Verified alert IDs 2293–2295 exist in PostgreSQL and join correctly to metric/governorate dimensions. Total anomaly_alerts rows: 87.
- Detector container restart_count=0 and OOMKilled=false.
- Telegram credentials are not configured (both presence checks false), so alerts are persisted locally but not sent to Telegram. This is optional unless external notifications are required for the next deployment stage.

- Post-rotation smoke DAG run `manual__fernet_rotation_smoke_20261124` completed successfully at 15:19:56 UTC. The task log confirms OpenAQ 7 valid / 0 rejected and Open-Meteo 2,592 valid / 0 rejected / 0 fetch failures. The new Fernet key therefore works with the live Airflow services and did not affect API ingestion.

- OpenAQ key rotation procedure confirmed against official OpenAQ documentation: sign in to OpenAQ Explorer account settings at https://explore.openaq.org/account and rotate the API key there. After the account owner provides the replacement key, update only the local `.env` value and recreate the four Airflow services, then rerun the ingestion smoke test.


## Post-Fernet end-to-end recheck and data-quality audit — 2026-11-24 15:24 UTC
- The post-rotation smoke ingestion produced 7 OpenAQ and 2,592 Open-Meteo valid records, all with zero rejects/fetch failures. Bronze persisted 7 OpenAQ rows, 1,296 air-quality rows, and 1,296 weather rows.
- Silver processed the new OpenAQ data in batch 61: 5 valid, 0 quarantined; Open-Meteo air-quality batch 55 was empty after deduplication. Gold batch 2000000061 committed 5 rows, and PostgreSQL loader loaded all 5 at 15:22:40 UTC.
- PostgreSQL is now at **92,765 fact rows / 27 Gold batches**. Data-quality SQL checks: 0 null keys/timestamps, 0 null aggregates, 0 non-positive measurement counts, 0 inverted min/max ranges, 0 averages outside min/max, and 0 duplicate grain groups.
- A transient PostgreSQL loader cycle at 15:22:24 UTC failed while enumerating the Gold directory with `OSError: [Errno 12] Cannot allocate memory`; the loader retried, recreated its serving schema check, and successfully loaded batch 2000000061 at 15:22:40 UTC. No container restart or OOM kill occurred. This is evidence of host/WSL memory pressure and must be treated as a reliability risk, even though retry recovered.
- Latest Docker stats showed Spark worker ~1.11 GiB, Silver ~386 MiB, Gold ~391 MiB, Bronze ~281 MiB, Kafka ~234 MiB, and Airflow services ~478 MiB combined. WSL swap remains essentially full; do not increase WSL memory or restart WSL while host available RAM is below 2 GiB.

- A non-disruptive WSL cache-reclaim attempt (`sync` then drop page/dentry caches) timed out with `Wsl/Service/WSAETIMEDOUT` before returning confirmation; do not assume caches were reclaimed. No WSL shutdown or Docker restart was performed. Immediate Compose recheck showed all 16 services still running.

- Final post-remediation regression run on host Python: `python -m pytest -q` → **153 passed in 11.08s**, exit code 0. `docker compose config -q` and `git diff --check` also passed (only LF→CRLF notices).

- PostgreSQL runtime health recheck: `airquality` and `airflow` showed 0 deadlocks, no active queries running longer than 2 minutes, and only one idle `airquality` backend at the sample. No DB lock/query backlog was observed.

- 15:30 UTC Silver interval recheck found no additional Bronze files after batch 61; all latest Silver commits remained at OpenAQ 61 / air_quality 55 / weather 53, with no uncommitted new batch or Gold backlog. PostgreSQL remained stable at 92,765 facts / 27 Gold batches. This is the expected idle state in the absence of a new ingestion run.


## Git/security and workspace cleanup update — 2026-11-24 19:55 UTC
- User rotated OpenAQ key. Host and Airflow scheduler/API fingerprints match; direct OpenAQ v3 request returned HTTP 200.
- Rewrote and force-pushed GitHub `main`, `fix/kafka-persistence`, and `master`. Removed `.env` / `.env.airflow-jwt` from reachable history and blanked credential-bearing environment assignments in all historical `.env.example` blobs. Verified 9 reachable commits, 0 private env paths, and 0 non-empty secret-like template assignments.
- Removed the redundant `Egypt-Air-Quality-Intelligence-Platform-GitHub` copy after verifying its HEAD was identical to the primary repo's Kafka-persistence branch. Preserved its three useful pre-Gold source snapshots under `backups/legacy-pre-current-gold-20261121/`.
- Removed 26 Python/test cache directories, the stale root audit output, and the one-off root OpenAQ smoke script. Untracked the Spark Ivy dependency cache from Git while leaving its local files available for Spark.
- The latest clean Git history bundle is `backups/git-history-cleanup-20261124/all-refs-after-full-secret-scrub.bundle`. The old secret-bearing Git metadata and intermediate bundle were removed after successful remote verification.
- Source changes remain uncommitted pending final review, regression tests, and stage/commit.


## Final project-file commit — 2026-11-24 20:05 UTC
- Consolidated the reviewed source, tests, documentation, and file-cleanup changes in commit `288ef5f` (`Finalize local platform integration and workspace hygiene`).
- Pushed `288ef5f` successfully to GitHub `origin/main`.
- Regression tests after cleanup: `153 passed in 25.68s`.
- `docker compose config -q`, `git diff --check`, and staged-file secret-assignment scan passed (0 non-placeholder secret assignments).
- CAPMAS reference GeoJSON parsed successfully as a FeatureCollection with 27 features.
- Working tree is clean and tracks `origin/main`.
- Refreshed the sanitized Git bundle after this commit; it verifies successfully and still contains 0 reachable private environment paths.

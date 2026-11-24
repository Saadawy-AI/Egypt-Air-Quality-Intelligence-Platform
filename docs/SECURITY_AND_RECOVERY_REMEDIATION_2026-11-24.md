# Security, Resource and Recovery Remediation — 2026-11-24

## Executive status

Local resource pressure was reduced, PostgreSQL role credentials and the Airflow Fernet key were rotated, and cold backup/restore drills passed for the lake/checkpoint tree and Kafka KRaft volume. The platform is running again after the maintenance windows.

This does **not** mean the project is cleared for Azure production. Three material blockers remain: the OpenAQ API key has not been revoked/replaced, the Airflow admin password remains the existing weak historical credential, and no off-host backup has been created. Kafka is still a single-broker RF=1 local setup.

## 1. Memory pressure

### Before
- WSL available memory: approximately 1.1 GiB.
- WSL swap: approximately 2.7–4.0 GiB used out of 4.0 GiB (it was observed full during the earlier peak).

### Changes
- Spark worker allocation reduced from 3 GiB / 5 cores to 2560 MiB / 4 cores.
- Spark Bronze/Silver/Gold driver heap capped at 512 MiB.
- Bronze/Silver/Gold executor core allocations remain 1 + 2 + 1, matching the four worker cores.
- Airflow LocalExecutor parallelism set to 4.
- Airflow scheduler parsing processes set to 1.
- No arbitrary Docker memory limits were added; those could kill streaming jobs.

### After
- WSL available memory: approximately 1.7 GiB immediately after the first tuning pass; final runtime check with all 16 containers Up: approximately 1.5 GiB.
- WSL swap: approximately 0.9 GiB immediately after the first tuning pass; final runtime check: approximately 2.0 GiB used out of 4.0 GiB.
- Airflow service RSS dropped materially after recreation; all services were observed running.
- Spark CPU was elevated immediately after restart while streams reconnected/caught up; monitor during the next sustained run.

## 2. Kafka replication and cold restore

- Local Kafka remains one broker, 3 partitions per application topic, RF=1. This is intentionally retained for the constrained local environment; changing RF to 3 on one broker would prevent topic creation/availability.
- A cold copy of the complete `egypt-air-quality_kafka_data` volume was created at `backups/2026-11-24/kafka-kraft-20261124.tar`.
- Archive SHA-256: `c0a412ba6b15779445d56a54db35c575503bf1703d433284c2ebebef2fd12079`.
- Isolated restore drill passed using a separate temporary volume and a separate Apache Kafka 4.1.0 broker.
- Restored topics were present; partition counts, topic IDs and end offsets matched the pre-backup snapshot exactly.
- Temporary restore container and volume were removed. The production Kafka broker, bridge and Spark writers were restarted.

### Azure/production requirement
Use a three-broker Kafka-compatible service/cluster across failure domains, RF=3 for application and internal topics, `min.insync.replicas=2`, producer `acks=all` and idempotence enabled, TLS/SASL, private networking, and broker/consumer-lag monitoring. Do not promote the local RF=1 topology to production.

Detailed evidence: `backups/2026-11-24/KAFKA_BACKUP_RESTORE_MANIFEST.txt` and `KAFKA_TOPIC_METADATA_BEFORE.txt`.

## 3. Bronze/Silver/Gold and Spark checkpoint backup

- Created `backups/2026-11-24/lake-checkpoints-20261124.tar`.
- Included `data/bronze`, `data/silver`, `data/gold`, `data/checkpoints`, `data/quarantine`, and `data/reference`.
- Spark Bronze/Silver/Gold writers were stopped before the snapshot and restarted afterwards.
- GNU tar archive comparison passed; extraction to a separate validation tree completed and required checkpoint metadata was present.
- Archive SHA-256: `8fe7c65a959d1430212f2eef399b6179a874c131bbb41d1a1c7e799d05bb700c`.
- The temporary extracted validation tree remains under `backups/2026-11-24/restore-validation-lake` as a restore-validation artifact; it is not used by runtime services and can be removed during housekeeping.

Detailed evidence: `backups/2026-11-24/LAKE_BACKUP_MANIFEST.txt`.

## 4. PostgreSQL backups and credential rotation

- Fresh pre-rotation custom-format dumps were created for `airquality` and `airflow`:
  - `airquality-pre-credential-rotation.dump`
  - `airflow-pre-credential-rotation.dump`
- Both dumps passed `pg_restore --list`.
- Rotated PostgreSQL role passwords for `postgres`, `airquality`, and `airflow`; updated the local `.env`; recreated the Postgres and dependent application containers.
- Credential audit confirmed the new database credentials authenticate.
- Rotated the Airflow Fernet key using the supported dual-key transition (new key first, old key retained for decryption), ran `airflow rotate-fernet-key`, then removed the old key from `.env`.
- Airflow database check passed; DAG import-error query returned no data.

### Admin credential — NOT ROTATED
The Airflow admin password reset was not completed. The local `.env` admin value was synchronized back to the last tracked value to avoid a mismatch with the existing Airflow user. That historical credential is weak and appears in Git history. Reset it in the Airflow UI or with the Airflow CLI during a controlled local session before Azure. Do not reuse it in production.

## 5. API key and Git history review

- The local `.env.example` was checked: API key field is blank and password/Fernet values are placeholders; no real credentials remain in that example file.
- `.env` is ignored locally and has been removed from the current Git index, but the Git history review found the tracked `.env` in five historical commits.
- The historical content includes the OpenAQ API key, Airflow Fernet key and weak Airflow admin credential.
- Database passwords and Fernet key have been rotated; the Airflow admin password and OpenAQ API key have **not** been rotated.
- The OpenAQ key must be revoked and replaced from OpenAQ Explorer account settings (`https://explore.openaq.org/account`). The official OpenAQ docs state that exposed keys should be replaced from account settings. Update the local `.env` directly; do not paste the new key into chat.
- A full automated Git-history secret scan (for example, Gitleaks) has not been completed because the scanner is not installed. Do not publish or push this repository until all exposed credentials are revoked and Git history is cleaned or a clean repository history is created.
- No Git reset, clean, history rewrite, commit or push was performed.

## 6. Off-host backup — BLOCKED

- Backups are currently on the same D: device as the project; they are not off-host.
- The connected Cloudflare account's R2 API reports that R2 storage is not enabled for the account. No remote bucket was created and no data was uploaded. Enable it in Cloudflare Dashboard → Storage & databases → R2 → Overview and complete the subscription checkout; then create a private bucket.
- No off-host backup can be uploaded until R2 is enabled and a backup-encryption key is stored securely; keep the local backups and treat off-host recovery as incomplete.

## 7. Runtime verification after maintenance

- All 16 project containers were observed Up after the maintenance/restart sequence; all had restart count 0 and OOMKilled=false in the final inspection.
- PostgreSQL fact table remained at 92,750 rows and 24 Gold batches at the last check.
- Airflow 12:00 UTC ingestion DAG run was successful; no DAG import errors were reported.
- Kafka topic end offsets did not advance during the immediate post-restart check; verify the next ingestion cycle creates new Bronze/Silver/Gold progress before declaring end-to-end recovery complete.
- The configured pytest suite previously passed: 153 passed, 0 failed, 3 subtests passed (218 dependency deprecation warnings).

## Acceptance gates still open

1. Revoke and replace the exposed OpenAQ API key.
2. Reset the Airflow admin password to a new strong value and verify Basic Auth.
3. Complete a full Git-history secret scan; remove/rewrite exposed historical secrets before sharing.
4. Enable R2 or another off-host destination; upload encrypted PostgreSQL, Kafka, lake and checkpoint backups; verify remote checksums.
5. Keep local Kafka RF=1 for development only; provision/validate RF=3 topology in Azure.
6. Observe at least one new post-maintenance Airflow → Kafka → Bronze → Silver → Gold → PostgreSQL cycle and verify batch/row-count advancement.


## Superseding update — 2026-11-24 19:55 UTC

The earlier credential/R2/Git statements above are historical and are superseded by this update.

- OpenAQ key rotation has now been performed by the account owner. The host `.env`, Airflow scheduler, and Airflow API server fingerprints match; a direct OpenAQ v3 request from the scheduler returned HTTP 200.
- Airflow's Fernet key was also rotated earlier; all four Airflow runtime services shared the current fingerprint and the post-rotation ingestion DAG smoke test succeeded.
- Cloudflare R2 is active. The bucket contains 11 backup objects; local/remote ContentLength and SHA-256 metadata were matched. PostgreSQL dumps passed isolated temporary-database restore drills; Kafka KRaft and lake/checkpoint archives passed separate isolated restore validation. See the dated manifests under `backups/2026-11-24/`.
- Git history was rewritten and force-pushed for `main`, `fix/kafka-persistence`, and `master`. All reachable `.env` / `.env.airflow-jwt` paths were removed; secret-bearing assignments in historical `.env.example` files were blanked. Direct Git-object verification found 0 private env paths and 0 non-empty secret-like template assignments across 9 reachable commits. See `docs/GIT_HISTORY_CLEANUP_2026-11-24.md`.
- The redundant `Egypt-Air-Quality-Intelligence-Platform-GitHub` directory was removed after verifying its Git HEAD was identical to the primary repository's Kafka-persistence branch. Its three useful pre-Gold source snapshots were preserved under `backups/legacy-pre-current-gold-20261121/`.
- Local Git status and file cleanup are in progress; the source changes have not yet been committed as a new project release.
- The last verified data-plane state was 92,765 PostgreSQL fact rows and 27 processed Gold batches, with 153 Python tests passing. Re-run tests after the final file cleanup before committing.
- Remaining release risks: host/WSL memory pressure and sustained soak/fault-injection testing; real MQTT sensor payload validation; Telegram is unconfigured; Kafka remains single-broker RF=1/PLAINTEXT and is development-only. Azure production is not approved yet.

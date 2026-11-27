# FIX_REPORT.md

## Verification date
2026-11-27 (UTC)

## Scope
Post-recreation verification of the Egypt Air Quality Intelligence Platform Docker Compose stack and Airflow. This report records observed evidence only. No persistent data, Docker volumes, Kafka offsets, or Spark checkpoints were modified during this verification.

## Current platform status

- Docker Compose stack: 18 core containers observed running.
- PostgreSQL: healthy.
- Kafka: healthy.
- Spark master and worker: healthy.
- Spark Bronze, Silver, and Gold applications: healthy.
- Airflow API server, scheduler, DAG processor, and triggerer: running.
- Airflow API health endpoint: metadatabase, scheduler, triggerer, and DAG processor all reported healthy.
- Airflow DAG import errors: none.
- Five DAGs are registered and unpaused.
- PostgreSQL Loader, anomaly detector, and Streamlit are running as UID 10001.
- Scheduler container: memory limit 1.5 GiB, OOMKilled=false, restart count 0.
- API server and DAG processor: memory limit 1 GiB each, OOMKilled=false, restart count 0.
- Kafka: memory limit 1.5 GiB, OOMKilled=false, restart count 0.
- Spark Silver: memory limit 1.25 GiB, OOMKilled=false, restart count 0.

## Verified successful DAG runs

### egypt_air_quality_platform
Run: scheduled__2026-11-27T06:15:00+00:00
State: SUCCESS

All eight tasks succeeded:
- preflight_kafka_spark_postgres
- ingest_sources_to_kafka
- wait_for_fresh_bronze
- wait_for_fresh_silver
- wait_for_fresh_gold_batch
- gold_data_quality_gate
- postgres_batch_and_rowcount_match
- platform_health_summary

Duration: approximately 9 minutes 16 seconds.

### egypt_air_quality_monitoring
Runs:
- scheduled__2026-11-27T06:30:00+00:00 — SUCCESS; all five tasks succeeded.
- scheduled__2026-11-27T06:35:00+00:00 — SUCCESS; all five tasks succeeded.

### egypt_air_quality_backup
Run: scheduled__2026-11-27T02:00:00+00:00 — SUCCESS.

The ingestion and ML DAGs had no runs in the queried date window; they were not manually triggered.

## Confirmed previous platform failure

Run: scheduled__2026-11-27T04:15:00+00:00
Failed task: gold_data_quality_gate.

The task log confirms an Airflow DAG import failure:
OSError: [Errno 12] Cannot allocate memory: '/opt/airflow/ingestion/config'

The traceback occurs during Python import resolution (importlib find_spec / _fill_cache) while the task runner loads the DAG bundle. This is not a Gold data-quality rejection.

Important timing: this failed run occurred before the latest Compose recreation. The 06:15 platform run succeeded before the recreation as well, so the evidence does not yet prove that the new memory limits have eliminated the intermittent error. A post-recreation platform run is still required.

## Airflow rendered-task-fields conflict

At 2026-11-27 06:30:52 UTC, the scheduler/API server logged HTTP 409 unique-constraint conflicts on rendered_task_instance_fields_pkey for several task instances in the monitoring DAG.

Despite these log entries:
- The 06:30 monitoring DAG run completed successfully.
- All five task instances are SUCCESS.
- The 06:35 monitoring DAG run also completed successfully.
- No repeated RTIF conflict was observed in the subsequent 10-minute log check.

The installed Airflow version is 3.0.6. This signature matches a known Airflow 3.x race condition involving concurrent writes of rendered task-instance fields. It is an Airflow metadata/API issue, not evidence of duplicate business data. No metadata-table cleanup or database mutation was attempted.

Recommended handling:
- Do not manually delete rows from rendered_task_instance_fields.
- Do not alter the metadata database schema.
- Plan an Airflow patch-version review/upgrade in a separate maintenance window after checking release notes, compatibility, and migration requirements.
- Keep monitoring task states; treat this as a known warning unless it begins causing task failures.

## Kafka startup timeouts

Kafka logged KRaft controller heartbeat/request timeouts around 06:29–06:30 UTC during startup. Kafka subsequently reported healthy, and monitoring DAG checks for Kafka succeeded at 06:30 and 06:35. No newer heartbeat timeout was found in the last 10-minute log query.

This appears transient during startup, but should be monitored if it recurs.

## Memory observations

At approximately 06:38 UTC, WSL reported:
- Total memory: about 5.8 GiB.
- Available memory: about 1.6 GiB.
- Free memory: about 240 MiB.
- Swap used: about 2.6 GiB.

Docker stats showed the Airflow scheduler using approximately 667 MiB of its 1.5 GiB limit during the sample. No OOMKilled status or container restart was observed for the inspected critical containers.

The available-memory figure and swap usage indicate the host remains under meaningful memory pressure. The previous OSError is not yet proven fixed.

## Actions taken in this verification

- Read-only checks of Compose state, Airflow health, DAG import errors, DAG runs, task states, service logs, memory usage, and container restart/OOM status.
- Verified UID 10001 for postgres-loader, anomaly-detector, and Streamlit.
- No source code or configuration changes made during this verification.
- No service restart/recreation performed.
- No DAG manually triggered.
- No Docker volumes, database records, Kafka topics/messages/offsets, or Spark checkpoints deleted or reset.

## Required next validation

1. Observe the next scheduled platform DAG run after this deployment (expected 07:15 UTC on 2026-11-27).
2. Confirm all eight tasks succeed, especially DAG import/task startup and gold_data_quality_gate.
3. Check Airflow scheduler/API logs for new OSError 12 and RTIF 409 entries.
4. Check Kafka KRaft heartbeat logs and container restart/OOM status.
5. Re-check PostgreSQL fact and processed-batch counts after the run.
6. If OSError 12 recurs, capture scheduler cgroup memory, host memory/swap, task subprocess RSS, and concurrent task count at the exact failure time before changing concurrency or memory limits.

## Overall conclusion

The Docker and Airflow stack is running and its health checks are passing. The 06:15 platform run and two subsequent monitoring runs succeeded. However, the previous memory-related DAG import failure has not yet been proven resolved because no post-recreation platform run has been observed. The RTIF unique-constraint conflict is a known Airflow 3.0.x race signature; it did not prevent the observed monitoring tasks from succeeding. The platform should be classified as operational but still under verification, not fully certified stable.

## Stability remediation — 2026-11-27 06:45 UTC

### Changes applied
- Removed the Airflow common-service bind mount `./ingestion:/opt/airflow/ingestion:ro`. The Airflow image already copies the ingestion package at build time. This avoids resolving/import-scanning the Windows/WSL bind-mounted `/opt/airflow/ingestion/config` tree, which was the exact path implicated in the prior intermittent `OSError: [Errno 12] Cannot allocate memory`.
- Set `max_active_tasks=1` on `egypt_air_quality_monitoring` to serialize its five independent health probes and reduce concurrent Airflow 3.0.x rendered-task-field API writes. The probes remain enabled; only their execution is serialized.
- Created rollback copies before editing:
  `../_audit_snapshots/pre_stability_fix_20261127T064521Z/docker-compose.yml`
  `../_audit_snapshots/pre_stability_fix_20261127T064521Z/egypt_air_quality_monitoring.py`

### Deployment and immediate verification
- `docker compose config -q`: passed.
- Python compile check for the modified monitoring DAG: passed.
- Rebuilt the Airflow image successfully.
- Recreated only Airflow API server, Scheduler, DAG Processor and Triggerer; all four started.
- Airflow API health endpoint reports metadata DB, scheduler, triggerer and DAG processor healthy.
- Airflow version remains 3.0.6.
- DAG import errors: none reported.
- Five expected DAGs are registered and all are unpaused.
- Scheduler container inspection confirms `/opt/airflow/ingestion` is no longer a bind mount; the package is supplied by the rebuilt image.
- All 18 core services are running; PostgreSQL and Kafka are healthy; Spark master/worker/Bronze/Silver/Gold health checks are passing.

### Still awaiting proof
- The next scheduled end-to-end platform DAG run after this remediation is required to confirm the ENOMEM mitigation under real workload. Do not call this resolved until all eight platform tasks pass.
- Observe the next monitoring DAG run to verify serial health probes stay within the 4-minute DAG timeout and whether RTIF 409 conflicts stop.
- This mitigation reduces the trigger surface for the observed Airflow issues but cannot guarantee 100% uptime. The Airflow 3.0.6 RTIF race remains an upstream-version risk pending a separately reviewed compatible patch upgrade.
- Host/WSL memory pressure and Windows-backed data-lake/checkpoint I/O remain architectural risks. Moving live data/checkpoints to Linux-managed storage requires a separately planned, checksummed migration and rollback; no such data migration was attempted here.

### Data safety
No Docker volumes, PostgreSQL records, Kafka topics/messages/offsets, Spark checkpoints, or project data files were deleted or reset. IoT/Wokwi services were not modified. No Git reset, clean, commit, or push was performed.

### Immediate post-restart sample (06:47 UTC)
- No new ENOMEM/OSError 12 or RTIF 409/unique-constraint messages in the first three minutes of Airflow logs after recreation.
- All 18 Compose containers are running; every inspected container has restart count 0 and OOMKilled=false.
- Sampled memory: scheduler ~329 MiB / 1.5 GiB; DAG processor ~287 MiB / 1 GiB; API server ~234 MiB / 1 GiB; triggerer ~362 MiB / 1 GiB; Kafka ~348 MiB / 1.5 GiB; Spark Silver ~341 MiB / 1.25 GiB.
- These are short-window observations only, not a soak test or proof of long-term stability.

## Follow-up verification — 2026-11-27 08:25 UTC

### Root cause of the remaining DAG import ENOMEM
The delayed 07:15 run log showed:
`OSError: [Errno 12] Cannot allocate memory: '/opt/airflow/dags'`.
This proves the earlier ingestion-mount removal was insufficient: the DAG directory itself was still a Windows-backed bind mount at the time of that run.

The active Compose configuration now has no bind mounts for `/opt/airflow/dags`, `/opt/airflow/plugins`, `/opt/airflow/scripts`, or `/opt/airflow/ingestion`. Those packages are copied into the Airflow image by `infra/docker/airflow.Dockerfile`. Live scheduler inspection confirms there is no host bind mount at those destinations. The scheduler has only Linux named volumes for project data/model candidates and bind mounts for non-import data such as logs, backups, model artifacts, and Kafka certificates.

### Post-fix end-to-end proof
Run: `scheduled__2026-11-27T08:15:00+00:00`
State: SUCCESS
Duration: approximately 8 minutes.

All eight tasks succeeded:
- preflight_kafka_spark_postgres
- ingest_sources_to_kafka
- wait_for_fresh_bronze
- wait_for_fresh_silver
- wait_for_fresh_gold_batch
- gold_data_quality_gate
- postgres_batch_and_rowcount_match
- platform_health_summary

Gold batch `2000000172` was written with 5 source rows, passed the Gold quality gate, and was loaded into PostgreSQL with 5 rows. The previous ENOMEM failure did not recur during this complete run.

A second manually triggered run was queued behind the scheduled run and began at 08:23 UTC. It is being monitored separately; it is not required to establish the successful 08:15 run.

### RTIF race remediation
- Verified the running scheduler's installed Airflow route contains the narrow RTIF primary-key collision handler from `infra/docker/patches/patch_airflow_rtif.py`.
- The handler catches only `rendered_task_instance_fields_pkey` IntegrityError, rolls back, reloads the TaskInstance, and retries the update. Other IntegrityErrors are re-raised.
- No RTIF unique-constraint/409 or ENOMEM messages were found in the last 10-minute scheduler log sample after the patch deployment.
- Airflow remains 3.0.6. Upstream Airflow 3.4.0 includes insert/update-on-conflict for rendered task instance fields (#63874); upgrading requires a metadata DB backup and migration plan, so no unreviewed schema upgrade was applied.

### Linux data lake/checkpoint volume
- Runtime uses external Docker local volume `egypt-air-quality_project_data`, mounted at `/opt/project-data`; its mountpoint is `/var/lib/docker/volumes/egypt-air-quality_project_data/_data`, not a Windows bind mount.
- Read-only inventory: approximately 793 MiB; Bronze 4,692 files; Silver 4,297; Gold 2,163; checkpoints 140,360 files.
- Migration archive `_migration_extras_20261127T0715Z` is present in the volume.
- Runtime Compose mounts for Spark Bronze/Silver/Gold, Airflow, PostgreSQL loader, and Streamlit use the named volume.
- SHA-256 manifest comparison against the retained Windows source directory is still pending; it will be run during a short Spark streaming pause to avoid a false mismatch while checkpoints/data are being written. No migration/reconciliation copy has been run during this verification.

### Memory pressure
- At 08:24 UTC, WSL reported MemTotal ~5.8 GiB, MemAvailable ~1.1 GiB, SwapTotal 4 GiB and SwapFree ~96 KiB.
- Windows reported ~1.3 GiB available physical memory at the same sample.
- No inspected container was OOM-killed or restarting; sampled Docker container RSS remained within configured limits.
- The host is still under memory pressure and showed active swap-in/swap-out activity. This is a remaining capacity risk, not an observed container OOM. Airflow global parallelism will be reduced after the queued validation run finishes; WSL memory settings will not be changed while the stack is live because applying them requires shutting down WSL/Docker.

### Safety
No Docker volumes, Kafka topics/offsets, PostgreSQL business records, or Spark checkpoints were deleted/reset. The project_data volume was not overwritten or reconciled during this follow-up. Optional IoT/Wokwi services were not started.

### Additional monitoring and storage verification — 08:32 UTC
- Monitoring DAG runs at 08:15, 08:20 and 08:25 UTC all completed SUCCESS. The 08:25 run's five tasks all succeeded: MQTT bridge, Kafka topics, Spark workers, PostgreSQL freshness and Gold DLQ.
- RTIF patch is present in both the running Scheduler and API Server images. No RTIF primary-key/409 or DAG import ENOMEM messages were found in the last 20-minute scheduler log sample.
- Read-only audit of the Linux named volume passed: Bronze 4,732 files; Silver 4,303; Gold 2,173; checkpoints 140,528. Required Bronze/Silver/Gold/checkpoint directories exist. Bronze checkpoint has 1,224 files, Silver 137,448, Gold notification checkpoint 408. Latest Gold batch `batch_id=00000000003000000119` has `_SUCCESS` and 8 Parquet files; sample SHA-256 hashes were readable.
- A separate source-vs-volume manifest scan was attempted read-only but the Windows source bind mount itself returned `OSError: [Errno 12] Cannot allocate memory` at `/src/bronze/air_quality/ingestion_date=2026-11-15`. The scan exited without changing either source or destination. This reinforces why the live platform must not scan the Windows source tree. Full source parity was not certified by that attempt; the existing migration archive and Linux-volume audit remain intact.
- Runtime inspection confirms Airflow Scheduler, Spark Bronze/Silver/Gold, PostgreSQL loader and Streamlit all mount `egypt-air-quality_project_data` at `/opt/project-data`; no Windows data bind mount is used by these services.

### Final follow-up — 2026-11-27 08:38 UTC
- The second manual platform run `manual__2026-11-27T08:16:16.478722+00:00_ju5c1CBK` also completed SUCCESS with all eight task instances successful. Its Silver wait found no new Silver batch after the 12-minute deduplication window, correctly returned the DAG's no-new-data success path, and the Gold/warehouse/health tasks completed successfully.
- Monitoring runs at 08:30 and 08:35 UTC both completed SUCCESS; all five monitoring task instances in each run succeeded.
- Reduced `AIRFLOW__CORE__PARALLELISM` from 4 to 2 to limit concurrent task memory/RTIF pressure. Backed up Compose before editing, validated `docker compose config -q`, and recreated only the four Airflow services. No database or data volume was recreated.
- After the tuning/restart: Airflow API health reports metadata DB, scheduler, triggerer and DAG processor healthy; Airflow parallelism reads back as 2; DAG import errors are empty. All 18 core containers are running.
- Memory sample after tuning: WSL MemAvailable ~1.38 GiB, SwapFree ~416 MiB; Windows available physical memory ~2.7 GiB. This is better than the earlier sample (WSL MemAvailable ~0.94 GiB, SwapFree <1 MiB), with no container OOM/restarts observed.

### Post-parallelism confirmation — 08:40 UTC
- Monitoring run `scheduled__2026-11-27T08:40:00+00:00` completed SUCCESS; all five health tasks passed.
- Airflow API health endpoint is healthy after the parallelism change. Core parallelism reads back as 2 and DAG import errors remain empty.
- No ENOMEM, RTIF unique-constraint, or 409 messages in the last 10-minute scheduler log sample.
- All 18 core services are running. Critical containers inspected (Scheduler, API Server, Spark Worker, Kafka, PostgreSQL) have restart count 0 and OOMKilled=false. Kafka, PostgreSQL and Spark master/worker/Bronze/Silver/Gold health checks are healthy.
- Optional IoT Simulator and Wokwi bridge remain stopped as they were before this work; they were not modified or started.

### Final live-state audit — 08:41 UTC
- Inspected every container returned by the active Compose project: all 18/18 are running, each with restart count 0 and `OOMKilled=false`.
- Scheduler's live mounts are only Linux named volumes for `/opt/project-data` and `/opt/model-candidates), plus non-code binds for model artifacts, Kafka certificates, backups and Airflow logs. There is no Windows bind mount for DAGs, plugins, scripts, ingestion code, Data Lake or Spark checkpoints.
- Airflow API health is healthy, import-error list is empty, core parallelism is 2, and the RTIF collision handler is present in the API Server image.
- The 08:40 monitoring DAG run succeeded after the Airflow restart/tuning.

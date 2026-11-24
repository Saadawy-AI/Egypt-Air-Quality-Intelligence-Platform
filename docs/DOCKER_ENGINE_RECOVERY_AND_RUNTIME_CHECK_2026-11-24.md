# Docker Engine Recovery and Runtime Check — 2026-11-24

## Executive result

The Docker Desktop Linux Engine API 500 observed during the previous final check was transient. A subsequent read-only check succeeded: Docker Server 29.9.3 responded, `docker compose ps` enumerated all 16 project services, and the Airflow, Spark Master, and Streamlit local HTTP endpoints returned HTTP 200. No Docker Desktop restart, WSL shutdown, container recreation, or volume operation was performed.

## Runtime evidence

- Docker Engine: Server 29.9.3; Docker context `desktop-linux`.
- Project services: 16/16 in running state.
- Healthy checks reported by Compose: PostgreSQL, Kafka, Spark Master, and Spark Worker.
- Local HTTP checks: Airflow API `127.0.0.1:8082/` = 200; Spark Master UI `127.0.0.1:8083/` = 200; Streamlit `127.0.0.1:8501/` = 200. Streamlit remains deferred as a product workstream; this was only a liveness check.
- Kafka topics present: `air-quality`, `openaq-measurements`, `silver-batch-notifications`, `weather` (plus `__consumer_offsets`).
- PostgreSQL application data: `fact_hourly_measurements` = 92,780 rows; `processed_gold_batches` = 30 rows.
- Latest Gold batch in PostgreSQL: batch 2000000068, 5 rows, processed at 2026-11-24 19:13:02 UTC.
- Latest fact update: 2026-11-24 19:13:02 UTC.
- Airflow ingestion DAG runs at 16:00, 17:00, 18:00, 19:00, and 20:00 UTC were successful. The 20:00 UTC run finished at 20:15:19 UTC.
- The successful 20:00 ingestion task reported OpenAQ 7 valid / 7 sent / 0 rejected / 0 failed and Open-Meteo 2,592 valid / 2,592 sent / 0 rejected / 0 failed.
- Spark Bronze and Gold containers each have RestartCount=2, but their latest state reports OOMKilled=false. They were running during the check.

## Resource pressure — release blocker

Host RAM was 15.79 GiB total, with only 0.69 GiB available (95.6% used) at the last sample. The user's `.wslconfig` currently sets WSL memory=6GB, swap=4GB, with `autoMemoryReclaim=dropcache`. The `vmmemWSL` process was using approximately 5.55 GiB. Docker's reported memory total was approximately 5.79 GiB.

Largest observed host processes included:
- `vmmemWSL`: approximately 5.55 GiB
- `ChatGPT Classic.exe`: approximately 895 MiB
- `vmmemWSA`: approximately 447 MiB
- `explorer.exe`: approximately 344 MiB
- Chrome processes: several hundred MiB each

The 20:00 UTC Airflow ingestion task's first attempt failed with `OSError: [Errno 12] Cannot allocate memory` while importing `/opt/airflow/ingestion/sources`. The task later succeeded on attempt 3. This confirms memory pressure is affecting task reliability even though the latest run succeeded. Do not call the local platform release-ready until memory headroom and retry behavior are improved and soak-tested.

## Throughput and latency warnings

- Spark Bronze logged micro-batches taking approximately 33.7 seconds against a 30-second trigger interval.
- Spark Silver logged file listings taking approximately 5–36 seconds for 76–279 files.
- MQTT/Kafka bridge emitted event-loop blocking warnings, including one task taking approximately 8 seconds. Kafka topics were present when checked; the bridge log's earlier metadata message showing zero topics did not represent the final Kafka topic inventory.
- Airflow scheduler emitted a BrokenPipeError in its log stream, but the 20:00 DAG run and ingestion task ultimately reached success. Investigate whether this recurs; do not classify it as resolved solely from the successful run.

## Actions taken

Read-only diagnostics only. No runtime configuration, secrets, data, checkpoints, volumes, or containers were modified. No destructive cleanup or service restart was performed.

## Required follow-up

1. Reduce host/WSL memory pressure and confirm at least 2 GiB host memory headroom during a full pipeline run.
2. Re-run the ingestion DAG under normal load and verify it succeeds without retries or allocation failures.
3. Investigate Spark micro-batch lag and Silver file-listing latency.
4. Profile the MQTT/Kafka bridge event loop and confirm real MQTT sensor payloads are received and forwarded.
5. Verify a new Gold batch is processed after the current latest batch and arrives in PostgreSQL.
6. Repeat a sustained soak test before Azure readiness sign-off.

## Conclusion

Docker Engine API responsiveness is currently restored and the full Compose project is running. The platform is **operational but not release-ready**: host memory headroom, ingestion retry behavior, Spark lag, bridge event-loop blocking, and end-to-end sensor acceptance remain open gates.

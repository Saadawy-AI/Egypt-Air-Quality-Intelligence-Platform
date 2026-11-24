# Pipeline Soak and Bronze/Silver/Gold Acceptance — 2026-11-24

## Result

**The requested local pipeline acceptance checks passed for the observed run.** This does not by itself certify the platform as Azure-ready; the remaining production-readiness gates are listed below.

## Test window and resource stability

- First uninterrupted monitor: 2026-11-24 22:38:19–23:10:11 (+02:00), 31 samples, approximately 32 minutes.
- Follow-on monitor: 2026-11-24 23:12:51–23:34:47 (+02:00), 21 samples, approximately 22 minutes.
- There was a 2 minute 40 second gap between the two sampling segments. The Docker services remained up, and the final inspection found no restarts.
- Combined evidence: 52 samples across a 56 minute 28 second wall-clock window.
- Available host RAM: minimum 2.021 GiB, maximum 2.806 GiB; observed RAM use ranged from 82% to 87%.
- All 16 Compose services were running at final inspection. Every container had RestartCount=0 and OOMKilled=false.
- Across the monitored window, no Spark Bronze trigger-overrun/falling-behind warning, Silver/Gold error, MQTT Event Loop Blocking warning, MQTT delivery failure, or queue-full warning was found in the checked logs.

## Acceptance checks

| Check | Evidence | Result |
|---|---|---|
| 30–60 minute endurance | 32-minute uninterrupted sample segment plus 22-minute follow-on; stable RAM and no service restarts | PASS |
| Bronze 30-second trigger | Trigger remains 30 seconds. For the new ingestion run, observed offset-to-commit times were approximately 7.8 seconds (air_quality), 13.7 seconds (openaq_measurements), and 12.3 seconds (weather). No falling-behind warning was found in the 60-minute log review. | PASS |
| Non-empty Silver | OpenAQ Silver batch 71: 5 valid rows, 0 quarantined; committed Parquet row count independently measured as 5. Air-quality and weather batches for that cycle were empty and correctly skipped. | PASS |
| Gold consumes new Silver batch | Gold logged dataset=openaq_measurements, silver_batch=71, gold_batch=2000000071, source_rows=5. Gold checkpoint advanced and the Gold batch directory was created. | PASS |
| PostgreSQL receipt and row-count match | Gold Parquet rows=5; processed_gold_batches row_count=5 for batch 2000000071; Silver Parquet rows=5. Loader logged successful load. | PASS |
| PostgreSQL fact update | fact_hourly_measurements increased from 92,785 to 92,790 rows; latest updated_at matched the new batch processing timestamp 2026-11-24 21:16:47.917598 UTC. | PASS |

## Source ingestion evidence

Airflow scheduled run `scheduled__2026-11-24T21:00:00+00:00` completed successfully in approximately 54 seconds:

- OpenAQ: 7 valid, 0 rejected, 7 sent, 100% publishing success.
- Open-Meteo: 2,592 valid, 0 rejected, 2,592 sent, 100% publishing success.

The Bronze files and Kafka checkpoints advanced after this run. No synthetic ESP32 payload was injected.

## Gold/PostgreSQL reconciliation

- Gold batch ID: `2000000071`
- Silver source batch: `openaq_measurements / 71`
- Silver Parquet row count: `5`
- Gold Parquet row count: `5`
- PostgreSQL `processed_gold_batches.row_count`: `5`
- PostgreSQL ledger rows: `32` (previously `31`)
- PostgreSQL fact rows: `92,790` (previously `92,785`)

The Silver Parquet count, Gold Parquet count, and PostgreSQL ledger count all match exactly for the new batch.

## Evidence files

- `artifacts/runtime_soak_2026-11-24.csv`
- `artifacts/runtime_soak_2026-11-24_part2.csv`
- `scripts/runtime_soak_monitor.py`
- `scripts/runtime_soak_monitor_part2.py`

## Remaining Azure-readiness gates

These soak checks pass, but the platform must not be described as fully Azure-ready until the separate production gates are completed:

1. Real ESP32 sensor payload receipt and validation end-to-end (no physical-device payload was tested in this run).
2. Kafka production security: TLS/SASL, ACLs, and secret/identity handling.
3. Kafka high availability: multi-broker deployment and appropriate replication/min-ISR policy; the local setup is single-broker RF=1.
4. Fault-injection and replay acceptance, including recovery from broker/worker interruption and checkpoint-safe reprocessing.
5. A longer production-like soak under representative sensor traffic and the target Azure resource envelope.

**Conclusion:** the six requested local soak and Bronze/Silver/Gold/PostgreSQL acceptance checks passed for this test run. Azure readiness remains conditional on the separate gates above.

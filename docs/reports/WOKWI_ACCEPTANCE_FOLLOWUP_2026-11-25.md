# Wokwi Acceptance Follow-up — 2026-11-25

## Scope
Follow-up to the Wokwi runtime validation covering Gold/PostgreSQL count reconciliation, latest Bronze delivery, real-time cadence, and the Streamlit IoT master lifecycle control.

## 1. Gold (7) vs PostgreSQL (8): explained — PASS
This is expected incremental aggregation, not a duplicate in Gold.

- Silver batch 71 contains one Wokwi record (the earlier message at Kafka partition 1 / offset 25016).
- Gold batch 1000000071 contributes measurement_count=1 for each of the five Wokwi metrics.
- Silver batch 72 contains seven Wokwi records (offsets 25061, 25064, 25067, 25070, 25077, 25080, 25083).
- Gold batch 1000000072 contributes measurement_count=7 for each of the same five metrics.
- PostgreSQL's FACT_UPSERT intentionally adds incoming measurement_count to the existing fact row on conflict. Therefore the serving fact count is 1 + 7 = 8.

PostgreSQL processed_gold_batches confirms batch 1000000072 with 20 rows. The classified fact table shows eight accumulated Wokwi observations for each of PM10, humidity, PM1, PM2.5 and temperature in the 23:00 UTC window. No corrective data mutation is required.

## 2. Latest Bridge messages in Bronze — PASS
The latest observed Wokwi records are present in Bronze dataset air_quality, ingestion_date=2026-11-24, topic air-quality, partition 1, kafka_key wokwi-esp32-01.

| Kafka offset | Kafka timestamp (UTC) | Bronze ingestion timestamp (UTC) |
|---:|---|---|
| 25088 | 23:29:51.291 | 23:30:00.007 |
| 25091 | 23:30:22.490 | 23:30:30.009 |
| 25094 | 23:30:56.174 | 23:31:00.005 |
| 25097 | 23:31:29.818 | 23:31:30.008 |
| 25100 | 23:31:55.609 | 23:32:00.004 |
| 25103 | 23:32:22.306 | 23:32:30.010 |

All six records are in Bronze. They are newer than the seven Wokwi records in Silver batch 72 and were not yet visible in a subsequent Silver batch at audit time.

Silver's Structured Streaming trigger is explicitly configured to processingTime=10 minutes in spark/silver/ingestion.py. Thus newly landed Bronze files may wait until the next 10-minute micro-batch. This is an intentional resource/throughput trade-off documented in the job; no checkpoint reset or service restart was performed.

## 3. Ten-second cadence — NOT PASSED in wall-clock time; cause identified
The local ESP32 sketch checks millis() against 10000ms. Six consecutive bridge arrivals (offsets 25088–25103) had wall-clock gaps of approximately 31.2s, 33.7s, 33.6s, 25.8s and 26.7s.

The corresponding device event timestamps advanced by approximately 10–12 seconds. This indicates the firmware's 10-second timer is operating in Wokwi simulated time, while the browser-based simulation is advancing at roughly one-third of wall-clock speed during this run. Bridge/MQTT/Kafka forwarding is not the source of the delay: the bridge and Kafka logs show the messages being forwarded immediately after each other at the same event timestamp.

No artificial 3-second firmware interval was introduced: doing so would make event timestamps and the intended 10-second measurement interval inaccurate and would compensate for a transient simulator/browser speed factor. The six-message wall-clock cadence acceptance test therefore remains failed. Re-test with the Wokwi tab foreground and simulation speed restored; if Wokwi still runs slower than real time, record it as simulator throughput rather than changing the physical-device sampling interval.

## 4. Six-message test — EXECUTED / FAIL
Six consecutive messages were present in Bridge/Bronze (offsets 25088, 25091, 25094, 25097, 25100, 25103). The measured wall-clock gaps above exceed the 10-second target, so the test did not pass.

## 5. Streamlit IoT master control — PASS
Used Streamlit AppTest against the deployed /app/app.py and exercised the actual master buttons:

1. Initial state: both optional services running; no Streamlit exceptions.
2. Clicked Turn OFF IoT. The app reran and reported: IoT layer OFF — both optional services are stopped.
3. Clicked Turn ON IoT. The app reran and reported: IoT layer ON — both optional services are running.
4. AppTest reported zero exceptions after both actions.

The Control API allowlist is restricted to python_iot -> iot-simulator and wokwi -> wokwi-mqtt-bridge. It does not accept core services as controllable sources. After the test, docker compose ps showed all core services running (Airflow components, Kafka, MQTT, both bridges, Spark Bronze/Silver/Gold/Master/Worker, PostgreSQL and loader, Streamlit, anomaly detector and source-control API). No core service was stopped or restarted by the master-button test.

## Final status
- Gold/PostgreSQL cumulative count reconciliation: PASS (expected additive upsert).
- Latest Wokwi messages in Bronze: PASS (six records, offsets 25088–25103).
- Silver downstream consumption of those six records: PENDING next 10-minute trigger.
- Ten-second wall-clock cadence: FAIL (Wokwi simulated-time vs wall-clock slowdown).
- Six-message wall-clock cadence acceptance: FAIL.
- Actual Streamlit master OFF/ON button exercise: PASS.
- Core services unaffected: PASS.


## Recheck after Silver batch 73 — 2026-11-25 01:42 EET

### Latest Bronze → Silver → Gold lineage
The six previously identified Wokwi messages (Kafka partition 1, offsets 25088, 25091, 25094, 25097, 25100, 25103) were consumed by Silver batch 73.
- Silver batch 73: 66 valid rows, 0 quarantined; all six Wokwi offsets above are present.
- Gold batch 1000000073: committed successfully, source_rows=66.
- Gold contains five Wokwi metrics (humidity, pm1, pm10, pm25, temperature), each with measurement_count=6.
- PostgreSQL loader had not yet recorded batch 1000000073 at the time of this recheck. PostgreSQL end-to-end acceptance for these six messages remains pending; do not infer it from Gold completion.

### Cadence interpretation correction
The previous section labelled the six-message cadence FAIL solely because it compared host/bridge wall-clock intervals. The ESP32 payload timestamps provide the device/simulation clock and are the correct clock for evaluating the firmware's 10,000ms millis()-based sampling interval in Wokwi.

For the six consecutive payload timestamps (23:15:13, 23:15:23, 23:15:35, 23:15:47, 23:15:57, 23:16:07 UTC), the measured intervals are 10, 12, 12, 10, and 10 seconds. Four of five intervals are exactly 10 seconds; two are 2 seconds over. This supports approximately 10-second cadence in simulation time, with small scheduling/clock granularity variance. It does not prove strict 10.000-second wall-clock delivery.

The matching Bridge/Kafka wall-clock gaps were 31.2, 33.7, 33.6, 25.8, and 26.7 seconds. The discrepancy between payload clock and host clock indicates Wokwi simulated time was progressing at roughly 0.35x wall-clock during this run. MQTT-to-Kafka forwarding timestamps are effectively immediate after bridge acceptance; no evidence points to Kafka/bridge queueing as the cause. Do not shorten the firmware interval to 3 seconds as a workaround: the simulator speed factor may vary and doing so changes the measurement interval in simulation time.

### Revised status
- Six messages in Bronze and Silver batch 73: PASS.
- Gold batch 1000000073 with six samples per Wokwi metric: PASS.
- PostgreSQL load for Gold batch 1000000073: PENDING at recheck time.
- Firmware sampling cadence: approximately 10 seconds in Wokwi simulation time (10/12/12/10/10 seconds); strict wall-clock 10-second delivery is not met because simulator time runs slower than host time.

## PostgreSQL completion — 2026-11-25 01:43 EET

Follow-up polling confirmed that PostgreSQL loaded Gold batch 1000000073 after 40 seconds. The loader log records 20 rows and processed_gold_batches contains (1000000073, 20).

The Wokwi fact rows for the 23:00 UTC window now show measurement_count=14 for each of the five metrics. This is the expected cumulative count: 1 observation from Silver 71 + 7 from Silver 72 + 6 from Silver 73 = 14. The separate 22:00 UTC window still has one earlier Wokwi observation.

Therefore the previously pending PostgreSQL check is now PASS. End-to-end lineage for the six newest messages is proven through Bronze → Silver 73 → Gold 1000000073 → PostgreSQL.

## Final updated Wokwi acceptance status
- Gold/PostgreSQL cumulative reconciliation: PASS (1 + 7 + 6 = 14 per metric for 23:00 UTC).
- Latest six Wokwi records in Bronze: PASS.
- Same six records in Silver batch 73: PASS.
- Gold batch 1000000073: PASS (20 total rows; six Wokwi observations per metric).
- PostgreSQL ledger and fact table: PASS (batch 1000000073 loaded; cumulative Wokwi count 14 per metric).
- Firmware sampling cadence: approximately 10 seconds in Wokwi simulation time (observed 10/12/12/10/10 seconds).
- Strict 10-second host wall-clock cadence: NOT MET because Wokwi simulated time was progressing at roughly 0.35x host wall-clock during this run.
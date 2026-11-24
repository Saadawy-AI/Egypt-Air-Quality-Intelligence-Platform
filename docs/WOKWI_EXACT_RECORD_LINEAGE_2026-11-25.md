# Wokwi Exact-Record Lineage Acceptance — 2026-11-25

## Result

**PASS** — the exact test message `wokwi-validation-04` was reconciled from Bronze through Silver and Gold into PostgreSQL.

The audit is reproducible with:

```bash
docker exec egypt-air-quality-postgres-loader-1 python /app/audit_wokwi_lineage.py
```

The script intentionally uses the existing read-only project-data mount and the loader's PostgreSQL connection environment. It does not modify pipeline data or database rows.

## Exact lineage

| Layer | Evidence |
|---|---|
| Bronze | `source=wokwi_esp32`, `device_id=wokwi-validation-04`, Kafka topic `air-quality`, partition `0`, offset `21413`, key `wokwi-validation-04` |
| Silver | Batch `69`; exact same Kafka partition/offset and device ID; record ID `wokwi_esp32:wokwi-validation-04:2026-11-24T22:40:02.052459Z`; governorate Cairo |
| Gold | Batch `1000000069`, derived from Silver batch `69`; five aggregate rows for the Wokwi source |
| PostgreSQL | `processed_gold_batches` contains batch `1000000069` with `row_count=5`; all five fact rows exactly match Gold on source, governorate, metric, window, average, minimum, maximum and measurement count |

## Measurement reconciliation

| Metric | Gold average/min/max | Gold count | PostgreSQL result |
|---|---:|---:|---|
| pm1 | 8.0 | 1 | Exact match |
| pm25 | 19.5 | 1 | Exact match |
| pm10 | 31.0 | 1 | Exact match |
| temperature | 27.0 | 1 | Exact match |
| humidity | 48.0 | 1 | Exact match |

All five PostgreSQL fact rows had `measurement_count=1`, and their average/minimum/maximum values exactly matched the corresponding Gold contribution. The audit exited with code 0 and printed `RESULT PASS`.

## Scope and interpretation

- This closes the **exact-record lineage gap** for the identified validation message: its Kafka coordinates and device ID survived Bronze → Silver, and the five resulting Gold metric contributions were reconciled exactly to PostgreSQL.
- Gold and the serving fact table remain intentionally aggregated; they do not persist the original per-message `device_id` or message ID as fact columns. The audit establishes the exact contribution by matching the Silver record's derived Gold aggregates to the corresponding PostgreSQL fact rows, not by claiming that PostgreSQL stores the original message ID.
- This particular payload was published by the validation MQTT test client. It is **not evidence that the Wokwi browser-based ESP32 simulation was launched**. Actual Wokwi UI/firmware acceptance and PlatformIO compilation remain separate outstanding checks.
- The particulate values originate from Wokwi potentiometer test inputs, not physical PM sensors. Temperature/humidity are simulated DHT22 values.

## Reproduction

The audit script is `postgres/audit_wokwi_lineage.py`. It verifies:
1. A unique Bronze raw payload containing `wokwi-validation-04`.
2. The same `device_id`, Kafka topic, partition and offset in Silver batch 69.
3. The five Wokwi Gold rows in batch `1000000069`.
4. The PostgreSQL processed-batch ledger and exact fact-value reconciliation.

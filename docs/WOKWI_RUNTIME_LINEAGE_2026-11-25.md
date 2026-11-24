# Wokwi Runtime Lineage Check — 2026-11-25

## Scope

This report records the first browser-simulation device message observed from device wokwi-esp32-01 and its verified progress through the local pipeline. It distinguishes this runtime message from the earlier synthetic validation record wokwi-validation-04.

## Verified: Wokwi -> MQTT -> Kafka -> Bronze

- Wokwi bridge accepted and republished telemetry at 2026-11-24T23:13:56.120Z.
- MQTT-to-Kafka bridge forwarded the same device message to Kafka topic air-quality.
- Bronze Parquet contains exactly one matching record for device_id=wokwi-esp32-01.
- Kafka coordinates: partition 1, offset 25016.
- Kafka key: wokwi-esp32-01.
- Device event timestamp: 2026-11-24T23:13:50Z.
- Record classification: source=wokwi_esp32, simulation=true, provider wokwi; location is Cairo, Egypt.
- Sensor values in the accepted sample: PM1 14.9094, PM2.5 38.91282, PM10 94.57143, temperature 28, humidity 55.
- Particulate values are potentiometer-derived synthetic test inputs, not physical or calibrated particulate-sensor readings.

## Not yet verified: Silver -> Gold -> PostgreSQL for this message

- Silver batch directories inspected through batch 70. No record matched both device_id=wokwi-esp32-01 and the Bronze Kafka coordinates (partition=1, offset=25016).
- Silver batch 70 processed 36 air-quality rows, but did not contain this Wokwi device record.
- No Gold aggregate derived from this runtime message was found.
- PostgreSQL still contains the earlier five-metric Wokwi aggregate at 2026-11-24 22:00:00Z, associated with the earlier validation exercise. It is not evidence that the new 23:13:50Z device message reached PostgreSQL.

Therefore this run is a PASS through Bronze and a NOT YET PROVEN result for Silver, Gold and PostgreSQL. Do not mark end-to-end Wokwi acceptance complete.

## Ten-second cadence check

The local firmware uses a 10,000 ms publish interval. However, the bridge log window inspected for this run contained only one accepted message for wokwi-esp32-01 and no second message. The configured interval is confirmed in source code, but sustained runtime cadence is NOT PASSED. Keep the Wokwi browser simulation actively running to collect at least six consecutive messages with approximately ten-second spacing before accepting the cadence test.

## Dashboard IoT master control

A dedicated IoT System Power expander was added above the individual source controls. Its Turn ON and Turn OFF actions operate only the optional Python IoT Simulator and Wokwi MQTT bridge through the existing authenticated internal Control API. Individual source controls remain available. Core API ingestion, Airflow, Kafka, MQTT, Spark and PostgreSQL are not included in the master switch.

Validation performed after deployment:
- Dashboard Python syntax compilation: PASS.
- Streamlit health endpoint: ok.
- Control API authenticated status request: PASS.
- Both optional IoT services reported running at inspection time.
- Docker Compose configuration validation: PASS.
- Streamlit rebuilt and recreated with --no-deps; core services were not recreated by this dashboard deployment.

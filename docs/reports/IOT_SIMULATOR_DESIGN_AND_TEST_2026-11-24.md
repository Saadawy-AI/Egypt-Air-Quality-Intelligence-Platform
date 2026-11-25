# Virtual IoT Sensor Simulator — 2026-11-24

## Architecture decision

A physical ESP32 is not a project requirement. The device-specific acceptance gate is replaced with a repeatable virtual IoT telemetry source. This preserves the IoT architecture exercise (device identity, telemetry schema, MQTT QoS 1, Kafka keying, streaming ingestion, validation, aggregation, and persistence) without claiming physical sensor measurements.

## Implementation

- `ingestion/iot/simulator/simulator.py` emits virtual PM1, PM2.5, PM10, temperature, and humidity measurements.
- Default fleet: three virtual nodes in Cairo, Giza, and Alexandria; device count and interval are configurable.
- MQTT topics: `egypt/air-quality/simulated/<governorate>/<device-id>`.
- QoS: 1; simulator waits for MQTT publish acknowledgement.
- Kafka bridge subscription already covers `egypt/air-quality/#` and forwards to `air-quality`, keyed by `metadata.device_id`.
- Every record uses source `iot_sensor_simulator` and metadata `simulation=true`, `device_type=virtual_iot_sensor`, profile name, and sequence number.
- Silver accepts this explicit simulator source using the same particulate-measurement validation and device-based deduplication path as physical sensor telemetry.
- Shared ingestion source registry accepts `iot_sensor_simulator`.
- The simulator is opt-in under Compose profile `iot-simulator`; it does not start during normal `docker compose up`.

## Run controls

One-shot (one record from each configured virtual node):

`docker compose --profile iot-simulator run --rm iot-simulator python -m ingestion.iot.simulator.simulator --once`

Continuous simulation (default 30-second cadence):

`docker compose --profile iot-simulator up iot-simulator`

Stop continuous simulation:

`docker compose --profile iot-simulator stop iot-simulator`

Configuration:

- `IOT_SIMULATOR_DEVICE_COUNT` (default 3)
- `IOT_SIMULATOR_INTERVAL_SECONDS` (default 30)
- `IOT_SIMULATOR_SEED` (default 20261124)
- `MQTT_SIMULATOR_TOPIC_PREFIX` (default `egypt/air-quality/simulated`)

## Test evidence

- Unit tests verify source label, simulation metadata, distinct virtual devices, measurement fields, and shared ingestion validation.
- Focused tests for simulator, MQTT bridge, and ingestion validation: 31 passed.
- `docker compose config -q`: passed.
- `git diff --check`: passed.
- One-shot simulator published 3 QoS 1 messages for virtual-air-node-001/002/003.
- MQTT bridge logs confirmed forwarding of all 3 records to Kafka topic `air-quality` with the expected device keys.

## Acceptance boundary

The simulator-to-MQTT-to-Kafka transport leg is verified. This does not constitute physical sensor validation, nor does it yet prove that these specific three records were committed through Bronze, accepted by Silver, aggregated by Gold, and reconciled in PostgreSQL. That downstream lineage check remains an explicit acceptance task. All simulated rows remain distinguishable by source and metadata and must not be reported as measured real-world air quality.

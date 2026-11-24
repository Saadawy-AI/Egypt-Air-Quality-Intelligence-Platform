# Optional IoT Source Control — 2026-11-25

## Source policy
Always-on API ingestion sources are OpenAQ, Open-Meteo Air Quality, and
Open-Meteo Weather. They are intentionally absent from the lifecycle API
allowlist and cannot be stopped from the dashboard.

Optional sources are the Python IoT Simulator and the Wokwi ESP32 MQTT bridge.
Their lifecycle is independent. A stopped optional source is not a pipeline
failure and does not stop core ingestion or processing services.

## Control path
Streamlit -> internal Control API -> filtered Docker Socket Proxy -> one
pre-created Compose container from a fixed allowlist.

The Control API is attached only to private Compose networks and has no host
port. Streamlit is connected to the private control network. The proxy exposes
container inspection and POST operations only; the API does not create
containers and does not accept arbitrary service names or Docker commands.
A random CONTROL_API_TOKEN is stored in the ignored local .env.

## Optional services
- Python IoT Simulator: Compose profile `iot-simulator`, service
  `iot-simulator`.
- Wokwi ingress: Compose profile `wokwi`, service `wokwi-mqtt-bridge`.
- Both containers are pre-created and then started/stopped through Streamlit.
- Wokwi public-broker ingress validates a high-entropy topic prefix, JSON
  schema, device-id format, timestamp and numeric sensor ranges before
  republishing into local Mosquitto under the existing simulated topic prefix.
- The bridge adds the configured Egypt governorate/coordinates so Silver's
  geographic validation can be applied.
- Wokwi records are explicitly tagged `source=wokwi_esp32` and
  `simulation=true`; Python simulator records remain tagged
  `source=iot_sensor_simulator` and `simulation=true`.
- Silver validation now explicitly accepts `wokwi_esp32` on the
  `air-quality` topic. Gold aggregation handles it through the existing
  non-OpenAQ measurement path.

## Provision / rebuild
```cmd
docker compose config --quiet
docker compose --profile iot-simulator create iot-simulator
docker compose --profile wokwi build wokwi-mqtt-bridge
docker compose --profile wokwi create wokwi-mqtt-bridge
docker compose up -d --build docker-socket-proxy source-control-api streamlit
```

The local .env contains CONTROL_API_TOKEN and WOKWI_TOPIC_PREFIX. The source
controller setup helper creates a random CONTROL_API_TOKEN; the Wokwi topic
prefix is also generated locally. Never commit .env or paste the prefix into
public issue trackers.

## Security boundary
The free Wokwi path uses an unauthenticated public MQTT broker over plaintext
port 1883. A random topic prefix and validation are defense-in-depth only;
this is not production-grade authenticated ingress. Do not publish secrets or
personal data. This path is for development/testing only. For production/Azure,
use authenticated TLS MQTT ingress and appropriate identity/authorization.

## Validation record
- Full existing project test suite: 157 passed.
- Control API + Wokwi bridge focused tests: 7 passed.
- Docker Compose configuration validation: passed.
- Python simulator lifecycle through Control API: Start confirmed running;
  three telemetry records published; Stop confirmed exited with code 0.
- Wokwi bridge lifecycle through Control API: Start and Stop confirmed.
- Wokwi bridge accepted public MQTT test messages and republished them to
  local Mosquitto; the existing MQTT-to-Kafka bridge confirmed forwarding to
  Kafka topic `air-quality`.
- One enriched Wokwi test record (`wokwi-validation-04`) was verified in
  Bronze Parquet with source, device id, simulation labels, Egypt location,
  coordinates, and Kafka offset.
- Silver validation was updated to accept Wokwi. The Silver query runs on a
  10-minute trigger; the new Wokwi Bronze record is awaiting the next
  scheduled Silver micro-batch. Therefore Wokwi Silver/Gold/PostgreSQL lineage
  is not yet claimed as complete.
- Streamlit HTTP endpoint returned 200. Final dashboard visual inspection is
  still pending.

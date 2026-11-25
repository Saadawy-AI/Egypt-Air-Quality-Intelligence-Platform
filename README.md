# Egypt Air Quality Intelligence Platform

An end-to-end data platform for collecting, processing, validating, aggregating, and exploring air-quality observations across Egypt. The repository is organized by component responsibility. Local runtime datasets, checkpoints, credentials, model binaries, logs and backups are preserved outside version control.

> **Status:** Local integration has demonstrated an end-to-end simulated Wokwi record path. Azure resources have not been deployed. Production readiness is not approved; see the Azure readiness review in docs/reports/.

## Architecture

OpenAQ, Open-Meteo Air Quality, Open-Meteo Weather and ESP32/Wokwi feed the ingestion layer. MQTT transports IoT messages into Kafka. Spark processes Bronze, Silver and Gold layers, with quarantine for invalid records. PostgreSQL serves the warehouse and Streamlit provides the dashboard. Airflow orchestrates scheduled tasks; Monitoring and Control API provide operational visibility and source controls.

Azure target (not deployed): Event Hubs, ADLS Gen2, Azure Database for PostgreSQL, managed identity/private networking and Azure Monitor.

Simulated IoT telemetry is explicitly labeled and must never be presented as calibrated ambient measurement.

## Repository structure

- ingestion/: openaq/, open_meteo_air_quality/, open_meteo_weather/, iot/wokwi/, iot/simulator/, common/, config/, processors/, producers/, consumers/, mqtt/
- kafka/: config/, schemas/, topics/, bridge/
- spark/: bronze/, silver/, gold/, jobs/, config/, checkpoints/
- data/: raw/, bronze/, silver/, gold/, reference/, quarantine/; runtime checkpoints, staging, logs and dependency cache are retained as additional directories
- airflow/: dags/, config/, plugins/ and local runtime logs
- postgres/: migrations/, schemas/, loaders/, audits/, tests/, init/
- streamlit/: pages/, components/, assets/
- ml/: training/, inference/, evaluation/, models/
- monitoring/: dashboards/, alerts/, exporters/
- control_api/
- testing/: unit/, integration/, end_to_end/, performance/, acceptance/
- infra/: docker/, azure/
- docs/: architecture/, guides/, operations/, reports/
- scripts/, artifacts/, backups/
- docker-compose.yml, .env.example, .gitignore and README.md

Kafka client adapters used by source ingestion remain under ingestion transport modules. The kafka/ directory contains broker configuration and bridge deployment assets; it intentionally has no Python package initializer because that would shadow the kafka-python dependency.

## Quick start

Prerequisites: Docker Desktop with WSL2 (Windows) or Docker Engine, Docker Compose v2, Git, and Python 3.11+ for local tests.

1. Clone the repository and enter its root.
2. Copy .env.example to .env and replace every placeholder with unique credentials.
3. Create the ignored .env.airflow-jwt file with independent JWT and web-session secrets as described in docs/guides/PLATFORM_GUIDE.md.
4. Validate and start:

```bash
docker compose config -q
docker compose up -d --build
docker compose ps -a
```

Optional IoT services are profile-controlled:

```bash
docker compose --profile iot-simulator up -d iot-simulator
docker compose --profile wokwi up -d wokwi-mqtt-bridge
```

Do not delete Docker volumes or Spark/Kafka checkpoints as routine troubleshooting.

## Local endpoints

| Service | Endpoint |
|---|---|
| Streamlit | http://localhost:8501 |
| Airflow | http://localhost:8082 |
| Spark Master UI | http://localhost:8083 |
| Spark Worker UI | http://localhost:8081 |
| Kafka | localhost:9092 |
| MQTT | localhost:1883 |
| PostgreSQL | localhost:5432 |

Verify actual service state with docker compose ps.

## Testing and quality

Run the deterministic offline regression suite and validate Compose:

```bash
python -m pytest -q
docker compose config -q
```

CI runs the Python suite, Compose validation and Azure Bicep compilation on pushes and pull requests to main. Passing CI does not establish production readiness.

## Azure status and cost guardrails

Selected region: UAE North. Initial Bicep compiles in CI, but Azure resources are not deployed. Review credit expiry, budget alerts, quotas, regional availability, security design, cost estimate and deployment what-if before any paid deployment. Azure Budgets are notifications, not hard spending caps. See infra/azure/README.md and docs/reports/AZURE_READINESS_REVIEW_2026-11-25.md.

## Documentation

Start at docs/INDEX.md, then use the architecture, guides, operations and reports sections.

## Security

Never commit .env, .env.airflow-jwt, API keys, tokens, passwords, private keys, connection strings or cloud credentials. Keep local MQTT/Kafka endpoints private. Preserve data provenance and simulation labels. Preserve streaming checkpoints during recovery.

# Egypt Air Quality Intelligence Platform

An end-to-end data platform for collecting, processing, validating, aggregating, and exploring air-quality observations across Egypt. The repository contains a local Docker-based data stack, an ESP32/Wokwi ingestion path, analytical dashboards, operational tooling, and an initial Azure infrastructure foundation.

> **Project status:** Local integration has demonstrated an end-to-end simulated Wokwi record path. Azure infrastructure has not been deployed. Production readiness is **not approved**; see the current [Azure readiness review](docs/AZURE_READINESS_REVIEW_2026-11-25.md).

## Architecture

```text
OpenAQ ───────────────┐
Open-Meteo Air/Weather ├─> Airflow / ingestion publishers ─> Kafka
ESP32 / Wokwi ─> MQTT ┘                                  |
                                                         v
                                             Spark Bronze -> Silver
                                                         |
                                                  Quarantine
                                                         |
                                                         v
                                                    Spark Gold
                                                         |
                                                         v
                                                PostgreSQL Warehouse
                                                         |
                                                         v
                                                 Streamlit Dashboard

Azure target (not deployed): Event Hubs + ADLS Gen2 + Azure Database
for PostgreSQL + managed identity/private networking + Azure Monitor.
```

Data layers retain source provenance. Simulated device telemetry must remain explicitly identified as simulated and must not be represented as calibrated ambient measurements.

## Repository map

| Path | Responsibility |
|---|---|
| `ingestion/` | Source clients, ingestion pipelines, Kafka producers/consumers, validation and MQTT/Wokwi adapters |
| `iot/` | ESP32 firmware and Wokwi project assets |
| `spark/jobs/` | Bronze, Silver and Gold Spark streaming jobs |
| `airflow/` | DAGs, plugins and Airflow image configuration |
| `postgres/` | Warehouse schema, initialization, migrations, loaders and audits |
| `streamlit_app/` | Analysis and machine-learning dashboards |
| `control_api/` | Authenticated source-control API and tests |
| `monitoring/` | Anomaly detection and alerting |
| `ml/` | PM2.5 model experiments and evaluation code |
| `infra/azure/` | Azure Bicep foundation and deployment guidance |
| `docs/` | Architecture decisions, runbooks, acceptance evidence and readiness reviews |
| `scripts/` | Backup, restore, configuration and runtime utilities |
| `data/reference/` | Versioned reference datasets and attribution |
| `artifacts/` | Selected, versioned test and runtime evidence |

Runtime data, checkpoints, local credentials, model binaries, logs, virtual environments and database backups are intentionally kept out of Git unless a specific artifact is explicitly approved for version control.

## Quick start (local development)

### Prerequisites

- Docker Desktop with WSL2 integration (Windows) or Docker Engine
- Docker Compose v2
- Git
- Python 3.11+ for local tests and development tools

### Configure

1. Clone the repository and enter its root directory.
2. Create a local environment file: copy `.env.example` to `.env`.
3. Replace every placeholder credential with a unique, strong value. Generate an Airflow Fernet key rather than reusing an example value.
4. Create the ignored `.env.airflow-jwt` file with independent JWT and web-session signing secrets, as described in [the full platform guide](docs/PLATFORM_GUIDE.md).
5. Review the Compose configuration and ensure the local resource limits are appropriate for your machine.

### Validate and start

```bash
docker compose config -q
docker compose up -d --build
docker compose ps -a
```

For the optional one-time ingestion bootstrap:

```bash
docker compose run --rm ingestion
```

The full setup instructions, service notes, data contracts, operational commands and analytical workflow are maintained in the [Platform Guide](docs/PLATFORM_GUIDE.md). Do not delete Docker volumes or Spark/Kafka checkpoints as a routine troubleshooting action.

## Local service endpoints

| Service | Local endpoint |
|---|---|
| Streamlit | http://localhost:8501 |
| Airflow | http://localhost:8082 |
| Spark Master UI | http://localhost:8083 |
| Spark Worker UI | http://localhost:8081 |
| Kafka (host access) | `localhost:9092` |
| MQTT | `localhost:1883` |
| PostgreSQL | `localhost:5432` |

Endpoints depend on the active Compose configuration; verify actual service state with `docker compose ps`.

## Tests and quality checks

Run the offline regression suite from the repository root:

```bash
python -m pytest -q
```

Validate Compose interpolation and service definitions after configuring local environment files:

```bash
docker compose config -q
```

CI runs the Python regression suite, Compose validation and Bicep compilation on pushes and pull requests targeting `main`. A passing CI run validates those checks only; it does not establish production readiness or deploy Azure resources.

## Azure status and cost guardrails

- Selected region: **UAE North**.
- Azure CLI login: verified against the enabled default subscription.
- Subscription role: **Owner**.
- Resource providers: Event Hubs, Storage and PostgreSQL are registered.
- Initial Bicep template: compiles in CI; deployment switches default to disabled.
- Azure resources: **none deployed**.
- The owner reported $190 in promotional credit and authorized up to $185 of use, with an alert requested when the remaining credit reaches $20. Credit expiry, billing treatment and effective alert configuration still require portal verification. Azure Budgets are notifications, not automatic spending caps.

Do not deploy or enable paid services until the credit/expiry details, budget alerts, quotas, regional availability, security design, cost estimate and deployment what-if have been reviewed and explicitly approved. See [Azure IaC guidance](infra/azure/README.md) and the [readiness review](docs/AZURE_READINESS_REVIEW_2026-11-25.md).

## Documentation

Start with the [documentation index](docs/INDEX.md). It groups architecture decisions, operational runbooks, Wokwi evidence, security and recovery notes, and Azure readiness materials.

## Security

- Never commit `.env`, `.env.airflow-jwt`, real API keys, tokens, passwords, private keys, connection strings or cloud credentials.
- Keep local MQTT/Kafka development endpoints private to the workstation.
- Treat Wokwi and other simulator readings as synthetic/simulated telemetry.
- Preserve provenance fields and streaming checkpoints during recovery.
- Report suspected credential exposure and rotate the affected credential; removing a value from the latest commit does not remove it from Git history.

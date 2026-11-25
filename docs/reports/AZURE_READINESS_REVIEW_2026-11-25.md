# Azure Readiness Review — 2026-11-25

## Executive decision

**Status: NOT READY FOR AZURE DEPLOYMENT.** The local data platform has a verified end-to-end Wokwi lineage through PostgreSQL and an independently tested IoT dashboard control. The current repository is clean and the Compose configuration validates. However, this is still a local Docker Compose implementation: there is no Azure infrastructure-as-code, no CI/CD workflow, no ADLS Gen2 storage adapter, no Azure identity/network configuration, and no Azure compatibility test environment.

This review is a planning and repository-readiness assessment only. No Azure resources were created, no subscription was accessed, and no cloud spend was incurred.

## Evidence checked in this review

- Repository: `main`, clean, tracking `origin/main` at inspection.
- `docker compose config -q`: PASS.
- `git diff --check`: PASS.
- Tracked file inventory: no Bicep, Terraform, ARM template, Kubernetes/Helm deployment, or `.github/workflows` pipeline files. Azure references in the tracked inventory are currently architecture/readiness documents only.
- `.env` and `.env.airflow-jwt` are not tracked; only `.env.example` is tracked. This does not replace a fresh Git-history secret scan before a public release.
- The Compose Kafka configuration is one combined KRaft broker/controller, PLAINTEXT listeners, and replication factor 1. The documented local Kafka metadata inspection also found no ACL entries.
- The MQTT broker configuration allows anonymous local MQTT access. The Wokwi ingress currently defaults to the public HiveMQ broker on port 1883; this is a development/test path, not a production device-ingress security design.
- README describes local `./data` mounts for Bronze/Silver/Gold/checkpoints and names ADLS Gen2 as a future cloud target. No ABFS/ADLS implementation is present in the tracked source inventory.
- The latest Wokwi acceptance record documents exact Bronze → Silver 73 → Gold 1000000073 → PostgreSQL lineage, and the dashboard IoT master switch was exercised OFF/ON without stopping core services.
- This review did not re-run a full data-plane soak or fault-injection drill.

## Gap register

| Area | Status | Evidence / gap | Required closure evidence |
|---|---|---|---|
| Local pipeline baseline | PASS (local scope) | Wokwi exact-record lineage is documented through PostgreSQL; Compose config validates. | Re-run acceptance suite against the final cloud-connected build. |
| Event streaming target | PARTIAL | ADR proposes Event Hubs Premium Kafka endpoint; no namespace, entities, client connectivity, throughput, or regional validation exists. | Non-production compatibility spike with real producer, Spark consumers, offsets, replay, throttling and batch reconciliation. |
| Event Hubs authentication | PARTIAL | ADR prefers Entra ID/OAUTHBEARER. Python bridge and Spark Kafka connector are not yet configured or tested against Event Hubs. | Test each client identity independently; prove least-privilege RBAC and negative authorization. |
| ADLS Gen2 lake | BLOCKED | Runtime lake and Spark checkpoints use local filesystem mounts. | Implement ABFS paths, managed identity auth, durable checkpoint layout, partitioning, retention, lifecycle and restore tests. |
| PostgreSQL target | PARTIAL | PostgreSQL schema, loader and a SQL migration exist locally; no Azure Database for PostgreSQL target or network/TLS configuration exists. | Validate Flexible Server schema migration, TLS certificate verification, private DNS/network, Entra auth or Key Vault credentials, loader idempotency and restore. |
| Compute/orchestration | BLOCKED | Current deployment is Docker Compose; Airflow uses LocalExecutor. No Azure hosting decision or deployment manifests exist. | Choose hosting for Spark, Airflow, MQTT bridge, loader, API and dashboard; document scaling, secrets, health checks and rollout/rollback. |
| Network isolation | BLOCKED | No Azure VNet/subnet/private endpoint/private DNS/NSG design or deployment code. | Approved network diagram and IaC; verify private-only data-plane connectivity and restricted egress. |
| Secret management/identity | BLOCKED | Local ignored env files are used. No Key Vault references or managed identities are implemented. | Per-service identity, Key Vault for unavoidable secrets, rotation/revocation drill, and no secret values in images, logs or Git. |
| IaC and CI/CD | BLOCKED | No Bicep/Terraform/ARM or GitHub Actions workflows are tracked. | Reproducible dev/test/prod IaC, lint/plan/what-if, tests, image build/scan/sign, gated deployment and rollback. |
| Container registry/supply chain | BLOCKED | Eight local Dockerfiles exist; no Azure registry or image promotion workflow is configured. | ACR (or approved registry), immutable image tags/digests, vulnerability scan, SBOM and deployment provenance. |
| Observability | PARTIAL | Local logs, health checks and anomaly detection exist; no Azure Monitor/Log Analytics/OTel export or alert rules are configured. | Central logs/metrics/traces, consumer lag, Event Hubs throttling, Spark progress, loader delay, dead-letter/quarantine and SLO alerts. |
| Backup and DR | PARTIAL | Local PostgreSQL/Kafka/lake restore evidence and R2 off-host backup evidence are documented. Azure-native retention, geo-redundancy and recovery objectives are not defined. | RPO/RTO, Azure backup policy, cross-region strategy, immutable copies, and timed full restore exercise. |
| Kafka HA/security | BLOCKED for self-managed Kafka; NOT REQUIRED if Event Hubs is selected | Local Kafka is single-node RF=1/PLAINTEXT. It must remain development-only. | If retaining Kafka: 3 controllers + 3 brokers, RF=3/min ISR=2, TLS/SASL/ACLs and failover tests. If Event Hubs: validate managed service behavior and do not port Kafka ACL assumptions. |
| Fault injection/replay | BLOCKED | The documented local acceptance does not include the full bridge/worker/PostgreSQL fault and isolated replay matrix. | Controlled, bounded fault/replay tests in isolated namespaces/checkpoints/database; prove no loss or double counting. |
| IoT source provenance | PASS (simulated path only) | Wokwi records remain explicitly classified as simulated; exact lineage to PostgreSQL is documented. | Preserve provenance in cloud schema and dashboards; never relabel Wokwi values as physical observations. Production device fleet requires its own identity, certificates and secure broker path. |
| ML model release | BLOCKED for production inference | README documents candidate models that failed the qualification gates and remain research-only. | Keep inference disabled until a new model passes locked-holdout/CV, drift and operational acceptance gates. |

## Target architecture recommendation

Retain the architecture decision already recorded in `AZURE_STREAMING_ARCHITECTURE_DECISION_2026-11-24.md`, with the following deployment boundaries:

1. **Event streaming:** Azure Event Hubs Premium Kafka endpoint as the preferred first target, conditional on region, networking, throughput, retention, quotas and client compatibility. Create one Event Hub entity for each current application topic through IaC. Do not treat Event Hubs as a fully interchangeable Kafka broker: validate unsupported admin operations, partition/consumer-group behavior, compression and transaction semantics.
2. **Lake:** ADLS Gen2 with hierarchical namespace, private access, ABFS URIs and managed identity. Preserve the existing Bronze/Silver/Gold contracts and deterministic batch IDs. Keep streaming checkpoints in a durable, isolated path with explicit backup/recovery policy; do not share a checkpoint between environments.
3. **Warehouse:** Azure Database for PostgreSQL Flexible Server is the preferred managed PostgreSQL candidate, subject to region, private networking, SKU, extension and cost validation. Keep the existing batch ledger and fact upserts transactional and idempotent.
4. **Compute:** decide between managed Spark (for example, Azure Databricks) and self-managed Spark on an Azure container/VM platform only after a compatibility and cost spike. Separately decide Airflow hosting; do not assume the local Compose/Airflow LocalExecutor topology scales as-is.
5. **Identity/network:** Microsoft Entra managed identities, least-privilege RBAC, Key Vault only for secrets that cannot be replaced with workload identity, private networking/private DNS, and TLS certificate verification.
6. **Operations:** Azure Monitor/Log Analytics, central structured logs, streaming lag/progress, pipeline freshness, quarantine counts, PostgreSQL loader latency, budget alerts and tested recovery objectives.

## Critical compatibility spike (before any production design lock)

Run this in a disposable non-production Azure environment after subscription, region and budget approval:

1. Provision Event Hubs Premium and the minimum required entities using IaC.
2. Connect the MQTT/Kafka bridge using its actual pinned Python Kafka client and an Entra token provider. The current code uses `kafka-python`; its OAUTHBEARER implementation and Azure token refresh behavior must be tested, not assumed.
3. Connect Spark 3.5.7's Kafka source to Event Hubs with the exact connector/client versions used by the project. Verify the required Java callback handler/dependencies and token refresh lifecycle.
4. Validate producer acknowledgements, partition assignment, consumer offset progression, restart/rebalance, bounded replay, gzip/none compression, and the absence of unsupported Kafka AdminClient/transaction dependencies.
5. Send a uniquely identified Wokwi test message and reconcile Event Hubs entity/partition/offset → Bronze record → Silver batch → Gold batch → PostgreSQL ledger/facts.
6. Repeat with a deliberately unauthorized identity and confirm access is denied. Test role revocation/rotation.
7. Measure throughput, throttling, latency and cost at representative and peak load.

Microsoft documents TLS as required for Event Hubs Kafka connections and Entra OAuth through SASL_SSL/OAUTHBEARER. The official migration guide recommends Premium or Dedicated for full Kafka protocol compatibility; verify the current tier feature matrix before choosing a tier. Python and Java clients need separate implementation tests.

## Implementation order

### Phase 0 — Decisions and sizing (no Azure resources)
- Confirm subscription, tenant, target region and data residency.
- Define expected average/peak messages per second, bytes per second, message-size distribution, partition count, consumer parallelism and retention/replay horizon.
- Set monthly budget ceiling, RPO/RTO, availability target and operational owner.
- Decide compute/orchestration hosting and the PostgreSQL target.
- Produce a threat model and private network/DNS design.

### Phase 1 — Reproducible foundation
- Add Bicep or Terraform modules for resource groups, VNet/subnets, private DNS, Event Hubs, ADLS Gen2, Key Vault, PostgreSQL, monitoring and registry.
- Add separate dev/test/prod parameter sets; use remote state with locking if Terraform is selected.
- Add CI for formatting/lint, unit/integration tests, IaC validation/what-if/plan, container scanning and artifact publishing.
- Implement workload identities and RBAC before application deployment.

### Phase 2 — Data-plane adapters
- Add environment-driven Kafka bootstrap/security configuration and test Event Hubs clients.
- Implement ADLS ABFS storage configuration for all lake outputs and checkpoints.
- Add Azure PostgreSQL connection configuration with TLS verification and secret/identity retrieval.
- Add schema migration execution as a controlled release step, not an implicit container startup side effect.
- Preserve batch ledger, deduplication, provenance and replay boundaries.

### Phase 3 — Non-production deployment and acceptance
- Deploy the minimum non-production stack.
- Verify positive and negative identity tests, private-only connectivity, end-to-end lineage, restart/replay, poison-message quarantine and loader idempotency.
- Run a production-like soak and record lag, latency, resource utilization, throttling and cost.
- Execute full backup/restore and fault-injection drills.

### Phase 4 — Production approval
- Review threat model, RBAC, secrets, network exposure, cost, SLOs, runbooks, RPO/RTO, restore evidence and rollback.
- Only then approve production provisioning/cutover.

## Decisions still required from the project owner

No cloud resources should be provisioned until these are supplied/approved:

- Azure subscription and tenant to use.
- Target region (and any data residency constraints).
- Monthly budget ceiling.
- Expected peak event rate / throughput and retention period.
- Preferred compute/orchestration hosting, or permission to compare options with a costed recommendation.
- Required availability target and RPO/RTO.

## External technical references

- Event Hubs Kafka protocol, TLS and authentication: https://learn.microsoft.com/azure/event-hubs/azure-event-hubs-kafka-overview
- Event Hubs Kafka migration and tier compatibility: https://learn.microsoft.com/azure/event-hubs/apache-kafka-migration-guide
- Event Hubs Entra ID/RBAC authorization: https://learn.microsoft.com/azure/event-hubs/authorize-access-azure-active-directory
- Event Hubs Python Kafka OAuth sample: https://github.com/Azure/azure-event-hubs-for-kafka/tree/master/tutorials/oauth/python
- ADLS Gen2 private access and ABFS documentation: https://learn.microsoft.com/azure/storage/blobs/data-lake-storage-introduction
- Azure Database for PostgreSQL Flexible Server networking: https://learn.microsoft.com/azure/postgresql/network/how-to-networking
- Azure Database for PostgreSQL TLS guidance: https://learn.microsoft.com/azure/postgresql/security/security-tls

## Final conclusion

The project is **locally integrated and suitable for a controlled Azure non-production compatibility spike**, but it is **not Azure deployment-ready**. The next safe engineering step is to implement IaC/CI foundations and prove Event Hubs + ADLS + PostgreSQL connectivity in a non-production subscription after the owner approves region and budget. No paid resources or deployment actions were taken in this review.


## CI foundation implementation update — 2026-11-25

- Added `.github/workflows/ci.yml` with two non-deployment jobs: the deterministic offline pytest suite and Docker Compose model validation.
- Compose validation creates only temporary CI placeholder environment files and runs `docker compose config -q`; no credentials or Azure access are required.
- Added `infra/azure/README.md` to define the IaC workspace, approved target direction, security/deployment guardrails, and explicit implementation checklist.
- No Azure deployment workflow or cloud provisioning permission was added. The workflow cannot create Azure resources.
- Local validation of the workflow itself is pending a GitHub Actions run. Docker Compose configuration was already validated successfully on the workstation during this session.
- The workstation does not have Azure CLI installed; Bicep compilation and subscription/region checks therefore remain unavailable locally. No Bicep templates are claimed as implemented.
- Working tree now contains the readiness review and CI/IaC planning changes; these have not been committed or pushed.


## CI foundation implementation update — 2026-11-25

- Added `.github/workflows/ci.yml` with two non-deployment jobs: the deterministic offline pytest suite and Docker Compose model validation.
- Compose validation creates only temporary CI placeholder environment files and runs `docker compose config -q`; no credentials or Azure access are required.
- Added `infra/azure/README.md` to define the IaC workspace, approved target direction, security/deployment guardrails, and explicit implementation checklist.
- No Azure deployment workflow or cloud provisioning permission was added. The workflow cannot create Azure resources.
- Local validation of the workflow itself is pending a GitHub Actions run. Docker Compose configuration was already validated successfully on the workstation during this session.
- The workstation does not have Azure CLI installed; Bicep compilation and subscription/region checks therefore remain unavailable locally. No Bicep templates are claimed as implemented.
- Working tree now contains the readiness review and CI/IaC planning changes; these have not been committed or pushed.

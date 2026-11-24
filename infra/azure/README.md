# Azure infrastructure workspace

This directory is reserved for Azure Infrastructure as Code and deployment
contracts. No Azure resources are provisioned by the repository at this stage.

## Current approved direction

See:
- `docs/AZURE_STREAMING_ARCHITECTURE_DECISION_2026-11-24.md`
- `docs/AZURE_READINESS_REVIEW_2026-11-25.md`

Preferred first target (conditional on compatibility, region, quota and cost
validation):
- Event Hubs Premium Kafka endpoint
- ADLS Gen2 with hierarchical namespace
- Azure Database for PostgreSQL Flexible Server
- Entra ID workload identities and narrowly scoped RBAC
- Key Vault only for secrets that cannot be replaced by managed identity
- Private endpoints/private DNS and Azure Monitor

## Deployment guardrails

1. Do not add subscription IDs, tenant IDs, credentials, SAS tokens, connection
   strings, or private keys to source control.
2. Do not run `az deployment`, create resources, or enable paid services until
   the project owner approves subscription, tenant, region and monthly budget.
3. Keep dev/test/prod isolated. Use separate state, storage paths, Event Hubs
   namespaces/entities, PostgreSQL databases and Spark checkpoints.
4. Provision Event Hubs entities through the Azure control plane; do not depend
   on Kafka AdminClient topic creation.
5. Deny public network access by default in production. Private endpoint and
   private DNS dependencies must be provisioned and tested before workloads.
6. Prefer managed identities and Entra ID. Any unavoidable secret belongs in
   Key Vault and must have a rotation/revocation runbook.
7. Never reuse local Docker Compose volumes or streaming checkpoints as cloud
   production state. Cloud checkpoint paths must be unique and durable.
8. Every infrastructure change must pass Bicep validation/what-if (or the
   selected IaC tool's equivalent) before deployment.

## Current implementation status

- [x] Azure target architecture and readiness gap register documented.
- [x] Offline Python regression and Compose-validation CI workflow added.
- [ ] Bicep modules and parameter files.
- [ ] VNet, delegated PostgreSQL subnet, private endpoint subnet and private DNS.
- [ ] Event Hubs namespace and four application entities.
- [ ] ADLS Gen2 account/filesystems and private endpoints.
- [ ] Key Vault, managed identities and RBAC assignments.
- [ ] PostgreSQL Flexible Server and private connectivity.
- [ ] Log Analytics, diagnostic settings, metrics and alerts.
- [ ] Non-production compatibility and recovery tests.

IaC templates are intentionally not marked deployable until network topology,
region, SKU/throughput sizing, availability requirements and budget are
approved. The current workstation has no Azure CLI installed, so local
subscription validation and Bicep compilation are not presently available.

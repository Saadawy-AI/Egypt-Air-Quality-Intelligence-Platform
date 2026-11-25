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


## Initial non-production template (not deployed)

The initial parameterized foundation is now available in main.bicep. It provides optional ADLS Gen2, Event Hubs Standard, and Log Analytics resources. All three deployment switches default to false, so a normal template invocation creates no billable resources unless explicitly enabled. This deliberately small compatibility-spike baseline is not the final production architecture; the preferred Premium Event Hubs target and private-network design remain subject to sizing, compatibility, region availability, and budget review.

The project owner selected UAE North. Azure CLI and Bicep are not installed on the connected Windows workstation as of the latest check. The owner reports $190 credit for one month and team-member credits of $100 for one year; these are credits, not an agreed recurring monthly spend cap. Subscription and tenant identifiers have not yet been supplied/verified.

See main.parameters.example.json for safe non-secret example parameters. Do not deploy until the resource group, subscription, tenant, credit ownership/expiry, cost alerts, quota and service availability are confirmed.

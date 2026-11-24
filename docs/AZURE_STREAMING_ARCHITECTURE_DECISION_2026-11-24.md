# Azure Streaming Architecture Decision Record — 2026-11-24

## Status
Proposed target for the first Azure deployment: **Azure Event Hubs Premium using its Kafka-compatible endpoint**. This is an architecture decision for planning, not a deployed or subscription-validated resource.

## Context
The local platform currently uses Apache Kafka 4.1.0 as one combined KRaft broker/controller with RF=1. That is a development topology, not an HA production design. The application uses Kafka producer/consumer APIs for MQTT bridge, Spark Bronze, and downstream notification flows.

## Decision
Prefer managed Event Hubs over operating a self-managed Kafka cluster for the initial Azure deployment, provided the region, network model, throughput, retention, feature compatibility, and cost checks pass.

Reasons:
- Managed service removes broker/controller operations and replica placement from the project team.
- Kafka protocol support permits reuse of compatible Kafka clients with endpoint/authentication configuration changes.
- TLS is mandatory; prefer Microsoft Entra ID OAuth/OAUTHBEARER and managed identities with least-privilege Azure RBAC.
- Private networking and zone-resilience options should be selected during provisioning, not bolted on after public exposure.

## Important compatibility constraints
Event Hubs is Kafka-compatible, not a drop-in self-managed Kafka cluster:
- Kafka topics map to Event Hubs entities; partitions and consumer groups need explicit sizing.
- Kafka AdminClient topic-management operations are not supported; provision entities using Azure control-plane tools (Bicep/ARM/Terraform/CLI).
- Retention and quotas differ by tier; size against peak throughput and replay horizon.
- Do not assume every Kafka broker setting, plugin, transaction, or exactly-once behavior is supported. Verify the current tier-specific support matrix before migration.
- Do not implement native Kafka SCRAM users/ACLs as the Azure auth model. Use Entra ID + RBAC/OAUTHBEARER where possible; SAS/SASL PLAIN is fallback only, with scoped policies and Key Vault-managed secrets.
- Kafka transactions have tier-specific preview/availability caveats; do not depend on them for correctness. Preserve idempotent batch IDs, durable checkpoints, deduplication, and PostgreSQL batch ledger semantics.

## Target Azure components
- Event Hubs Premium namespace, Kafka endpoint, private endpoint/VNet integration, zone redundancy where supported in the selected region.
- Separate Event Hubs entities for current application topics: air-quality, weather, openaq-measurements, silver-batch-notifications. Create through IaC.
- Separate managed identities for ingestion/producer, MQTT bridge, Bronze, Silver/Gold, and operational deployment identities.
- Entra ID OAuth with OAUTHBEARER and narrowly scoped Data Sender/Data Receiver roles at the narrowest supported entity scope.
- Azure Key Vault for any unavoidable non-managed-identity secrets; managed identity for service-to-service access.
- Azure Monitor diagnostics/metrics, alerts for throttling, server errors, consumer lag/processing delay, and capacity.
- Event Hubs Capture or existing Spark/lake sink for long-term immutable raw retention; define retention and replay policy independently from event streaming retention.

## Migration and acceptance
1. Inventory actual topics, partitions, peak ingress/egress, message size, retention, consumer groups, offsets, and producer/consumer API features.
2. Provision a separate non-production namespace with IaC, private networking, Entra ID auth, and required entities.
3. Validate each client independently with its own managed identity and RBAC role; negative-test missing role and wrong identity.
4. Validate the exact end-to-end pipeline and simulator lineage; compare partition offsets, source counts, batch IDs, checkpoint progression, and PostgreSQL ledger/facts.
5. Run dual-publish or controlled cutover only with a documented replay boundary and rollback plan. Do not point existing production consumers at a new endpoint without reconciling offsets.
6. Load-test against target throughput/partition counts; validate throttling behavior and retry/backoff.
7. Run a production-like soak and backup/restore/recovery exercise before cutover.

## Alternatives
### Self-managed Apache Kafka on Azure
Use only if Event Hubs incompatibilities are proven blockers. Requires at least three independent KRaft controllers and three brokers across failure domains, RF=3, min ISR=2, TLS/SASL, ACLs, persistent disks, broker replacement procedures, monitoring, and tested DR. This has materially higher operational burden.

### Keep the local single-node broker
Accepted only for local development/test. It is not a production Azure target and must not be described as HA or secure.

## Open decisions / inputs required before provisioning
- Azure subscription and target region.
- Expected peak messages/sec, bytes/sec, message-size distribution, and growth.
- Required replay/retention horizon and data residency.
- Private networking topology and DNS integration.
- Monthly budget and cost ceiling.
- Whether Spark Kafka connector version and all used Kafka client operations pass a compatibility spike against Event Hubs.

## References
- Microsoft Learn, Event Hubs Kafka overview: https://learn.microsoft.com/azure/event-hubs/azure-event-hubs-kafka-overview
- Microsoft Learn, Kafka client configurations and limitations: https://learn.microsoft.com/en-in/azure/event-hubs/apache-kafka-configurations
- Microsoft Learn, Event Hubs Kafka migration guide: https://learn.microsoft.com/en-us/azure/event-hubs/apache-kafka-migration-guide
- Microsoft Learn, Event Hubs tiers: https://learn.microsoft.com/en-us/azure/event-hubs/compare-tiers
- Microsoft Learn, Kafka authentication with Entra ID: https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-quickstart-kafka-enabled-event-hubs?tabs=passwordless

## Current status
No Azure resources were provisioned by this decision. Gate 1 exact simulator lineage remains open. Azure deployment must remain blocked until subscription/region/cost inputs and the compatibility spike are complete.

# Kafka Production High Availability Requirements

Status: design/acceptance requirements only; local single-broker Compose remains RF=1.

## Required topology
- Three independent Kafka brokers, distributed across separate Azure availability zones or equivalent failure domains.
- Three replicas for application topics and Kafka internal topics.
- `min.insync.replicas=2`; producers use `acks=all`, retries and idempotence.
- Disable unclean leader election.
- Replicate consumer offsets and transaction state with RF=3 and minimum ISR=2.
- Persistent broker storage with capacity and IOPS sized from measured throughput/retention.

## Security
- TLS for client and broker traffic.
- SASL/SCRAM or managed identity-supported authentication; no anonymous PLAINTEXT listeners.
- Private endpoints/VNet integration and least-privilege topic ACLs.
- Store credentials and certificates in Azure Key Vault; rotate them.
- Separate service identities for ingestion, Spark Bronze/Silver/Gold, and operational tooling.

## Reliability and observability
- Alert on under-replicated partitions, offline partitions, ISR shrink, broker unavailability, disk pressure, request latency and consumer lag.
- Monitor Structured Streaming progress and checkpoint health; Kafka consumer-group lag alone is insufficient when Spark reads Kafka directly.
- Test broker loss, leader election, rolling upgrades, throttling/backpressure, and recovery from checkpoints.
- Define retention and replay windows; ensure idempotent Gold loading and deduplication on replay.
- Use cross-region replication or a documented recovery strategy for regional failure.

## Acceptance tests before production
1. RF=3 and min ISR=2 verified for all application and internal topics.
2. Stop one broker: producers with acks=all continue while two replicas remain in ISR.
3. Stop two brokers: writes fail safely rather than acknowledging under-replicated data.
4. Restart brokers: partitions return to healthy ISR and no acknowledged records are lost.
5. Replay from Spark checkpoints: no duplicate Gold rows or lost batches.
6. Validate TLS/SASL, ACL denial tests, secret rotation and monitoring alerts.

The local single-broker RF=1 setup is suitable for development/testing only. Do not change local RF to 3 while only one broker exists; that would leave topics unavailable.

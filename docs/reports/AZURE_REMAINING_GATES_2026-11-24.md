# Remaining Azure Readiness Gates — 2026-11-24

## Current verified baseline

Repository: `main`, HEAD `45c9623`, clean at inspection.
All 16 Compose services were running at inspection.
Kafka runtime is Apache Kafka 4.1.0 in a single combined broker/controller container.

The live Kafka metadata inspection found:
- `air-quality`, `weather`, `openaq-measurements`, and `silver-batch-notifications`: 3 partitions each, replication factor 1, min.insync.replicas=1.
- `__consumer_offsets`: replication factor 1.
- Kafka ACL listing returned no ACL entries.
- The Compose Kafka listeners are PLAINTEXT (internal 29092 and host 9092); no TLS or SASL listener is configured.
- MQTT broker configuration currently allows anonymous connections on port 1883.
- The MQTT bridge forwards to Kafka topic `air-quality`; its subscribed filter is `egypt/air-quality/#`.

These are development settings, not production security or HA.

## Gate 1 — Virtual IoT telemetry to PostgreSQL

Decision: a physical ESP32 is explicitly out of scope. The project will use an opt-in virtual IoT sensor fleet. This is valid for testing the IoT messaging and data platform, but simulated measurements must always remain labeled and must never be represented as real environmental observations.

Implemented:
- Optional Compose profile `iot-simulator` with three virtual nodes (Cairo, Giza, Alexandria).
- QoS 1 MQTT publication to `egypt/air-quality/simulated/...`.
- Each message uses source `iot_sensor_simulator` and metadata `simulation=true`, `device_type=virtual_iot_sensor`, device ID, profile, and sequence.
- MQTT bridge forwards to Kafka `air-quality`, keyed by virtual device ID.
- Silver validation and deduplication accept the explicit simulator source.
- Shared ingestion source registry accepts `iot_sensor_simulator`.
- Unit tests and one-shot MQTT-to-Kafka transport test passed (see `IOT_SIMULATOR_DESIGN_AND_TEST_2026-11-24.md`).

Acceptance status: MQTT-to-Kafka transport is verified for three virtual devices. The complete lineage for those exact records through Bronze, Silver, Gold, and PostgreSQL is still pending; do not mark this gate fully passed until batch IDs and row counts are reconciled in PostgreSQL. A physical ESP32/COM port is not required.

Run one-shot: `docker compose --profile iot-simulator run --rm iot-simulator python -m ingestion.iot.simulator.simulator --once`
Run continuously: `docker compose --profile iot-simulator up iot-simulator`
Stop: `docker compose --profile iot-simulator stop iot-simulator`

## Gate 2 — Kafka TLS/SASL and ACLs

Status: NOT IMPLEMENTED. Runtime is PLAINTEXT and ACL listing is empty.

Implementation requirements (do this as a coordinated migration, not a partial listener flip):
- Use TLS for every client and inter-broker connection; validate broker certificates and hostnames.
- Use SASL/SCRAM-SHA-512 for service identities, or mTLS identities if the target platform's identity model supports it. Store credentials in a secret manager, not Compose literals, Git, images, or logs.
- Create distinct principals for Airflow ingestion, MQTT bridge, Spark Bronze, Spark Silver/Gold, and operational administration.
- Grant least-privilege topic permissions: producers WRITE/DESCRIBE only to their required topics; consumers READ/DESCRIBE and required consumer-group access; administrators receive separate audited privileges.
- Include ACLs for internal topics and transactional/idempotent producer requirements where used.
- Update kafka-python bridge and every Spark Kafka source with matching TLS/SASL settings; test each principal independently.
- Add negative tests: unauthenticated connection rejected, wrong password rejected, unapproved topic read/write rejected, valid principal allowed only its assigned operations.
- Rotate credentials and prove revocation of the previous credential.

Do not enable `allow.everyone.if.no.acl.found` as a substitute for defining ACLs. Do not remove PLAINTEXT until all clients have migrated and passed authenticated connectivity tests.

## Gate 3 — Kafka high availability

Status: NOT IMPLEMENTED. Current broker and controller are one node; all application topics and `__consumer_offsets` have RF=1/min ISR=1.

Target self-managed topology:
- Three independent KRaft controller nodes for a quorum.
- At least three independent brokers across separate failure domains.
- Application topics: replication factor 3, min.insync.replicas 2, producer `acks=all`, retries enabled, and idempotence enabled where compatible.
- Internal offsets and transaction-state topics configured with replication factor 3 and min ISR 2.
- Spread brokers across availability zones; use persistent managed disks, capacity monitoring, and tested broker replacement.
- Reassign existing partitions and verify ISR=3 before changing min ISR. Never simply change RF settings on the current single broker and assume replicas exist.
- For Azure, compare operating a three-broker Kafka cluster against a managed Kafka-compatible service; the final design must include zone redundancy, private networking, monitoring, backup/restore, and cost sizing.

Migration safety:
1. Take and verify a recoverable Kafka/data backup plus the existing lake/checkpoint backup.
2. Provision the new cluster separately; do not attach the existing single-node volume to multiple brokers.
3. Create topics with RF=3 and min ISR=2; provision principals/ACLs first.
4. Migrate producers and consumers gradually, preserving consumer offsets and validating replay boundaries.
5. Reconcile topic/partition offsets and downstream batch IDs before retiring the old broker.

## Gate 4 — Fault injection and replay

Status: NOT RUN. No broker/worker failure or replay acceptance evidence was produced in this session.

Required controlled test matrix:
- Pause/stop the MQTT bridge briefly; confirm QoS 1 messages remain unacknowledged or are redelivered, then verify recovery and no silent loss.
- Interrupt a Spark worker while Bronze/Silver/Gold are processing; verify checkpoints remain intact and the job resumes.
- Interrupt one Kafka broker in the future HA cluster; verify leadership/ISR failover and continued produce/consume with RF=3/min ISR=2.
- Disconnect PostgreSQL temporarily; verify Gold output remains recoverable and loader retries without marking an uncommitted batch complete.
- Replay a bounded, known offset range into an isolated test namespace/database; compare source counts, deduplication behavior, batch ledger, and fact-table uniqueness.
- Test poison/malformed payload quarantine and confirm valid neighboring messages continue.
- Record injected fault start/end, expected behavior, observed recovery time, offsets, checkpoint paths, batch IDs, row counts, and duplicate/loss reconciliation.

Safety constraints:
- Never delete or reset production checkpoints to force replay.
- Never truncate production topics or tables for a test.
- Run destructive replay only against isolated topics, checkpoint directories, and a disposable database.
- Start with bridge/worker interruption; do not kill the only active Kafka broker until a maintenance window and verified recovery point are approved.
- Each test must have a rollback/restore step and a clear pass/fail criterion.

## Immediate next actions

1. Complete the exact virtual IoT record lineage from MQTT/Kafka through Bronze, Silver, Gold, and PostgreSQL; reconcile simulator device IDs, source labels, batch IDs, and row counts. Physical ESP32/COM hardware is out of scope.
2. Adopt Azure Event Hubs Premium with Kafka endpoint as the preferred managed Kafka-compatible target for the initial Azure deployment, subject to subscription/region/throughput and cost validation. Use private networking and TLS/SASL_SSL with the Event Hubs connection-string mechanism; authorize through narrowly scoped Event Hubs authorization rules or Azure RBAC where supported. Do not assume Kafka SCRAM users/ACL semantics map one-to-one to Event Hubs.
3. Keep local single-node Kafka explicitly development-only. If self-managed Kafka is selected instead of Event Hubs, implement TLS/SASL and ACLs together with a three-controller/three-broker KRaft topology, RF=3, min ISR=2, and verified migration; never fake HA by changing RF on one broker.
4. Migrate producer/consumer clients to the selected Azure endpoint and test authenticated positive/negative access, secret rotation/revocation, private connectivity, and offset/batch reconciliation.
5. Execute the isolated bridge/worker/PostgreSQL fault and bounded-replay matrix locally; perform broker failover tests only on the future multi-node target, never by killing the sole local broker.
6. Run a production-like Azure soak, validate backup/restore and disaster recovery, then complete the deployment/security/cost sign-off.

## Current decision

The local Bronze/Silver/Gold/PostgreSQL soak acceptance remains passed as documented in `PIPELINE_SOAK_ACCEPTANCE_2026-11-24.md`. The four gates in this document remain open. Do not label the platform Azure-ready until all four gates and the production-like Azure soak pass.

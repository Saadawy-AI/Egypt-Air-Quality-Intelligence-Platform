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

## Gate 1 — Real ESP32 to PostgreSQL

Status: BLOCKED pending physical device connection and a real sensor transmission.

The Windows serial-port registry contained no serial-port mappings during this inspection. Python's pyserial module is also not installed in the host interpreter. This does not prove the board is defective; it means there is currently no observable COM-port path from which to flash or read the ESP32.

Acceptance procedure once the board is connected:
1. Connect the ESP32 by a known data-capable USB cable; confirm a COM port and record VID/PID and firmware version.
2. Confirm firmware publishes QoS 1 JSON to `egypt/air-quality/<device-id>`.
3. Payload must contain a stable `device_id`, event timestamp, schema/version, and sensor measurements with units.
4. Subscribe/read-only observe the device topic; capture the real MQTT message and timestamp. Do not inject fabricated data into the production topic.
5. Verify bridge delivery to Kafka `air-quality`, including the device key.
6. Verify Bronze offset/checkpoint and Parquet record; then verify Silver validation/quarantine outcome.
7. Verify the resulting Gold batch ID and row count in `processed_gold_batches` and matching fact rows in PostgreSQL.
8. Preserve a redacted evidence record (payload, device ID, timestamps, Kafka partition/offset, Bronze/Silver/Gold batch IDs, SQL counts). Never include Wi-Fi passwords or broker credentials.

Pass criteria: one real board-originated record is traceable through every stage with matching identity/timestamps and no unexplained row loss or duplication.

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

1. Connect the actual ESP32 and provide its firmware/project or confirm the COM port appears; then run Gate 1 with a real message.
2. Choose the target Azure Kafka model (managed Kafka-compatible service vs self-managed KRaft) before implementing Gate 2/3. The security and HA topology must be designed together.
3. Implement client authentication/ACLs and migrate consumers/producers as one tested change.
4. Provision multi-broker HA and migrate topics with verified replication.
5. Execute the isolated and then maintenance-window fault/replay matrix.

## Current decision

The local Bronze/Silver/Gold/PostgreSQL soak acceptance remains passed as documented in `PIPELINE_SOAK_ACCEPTANCE_2026-11-24.md`. The four gates in this document remain open. Do not label the platform Azure-ready until all four gates and the production-like Azure soak pass.

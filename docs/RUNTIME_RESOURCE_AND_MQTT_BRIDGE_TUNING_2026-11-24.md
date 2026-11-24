# Runtime Resource and MQTT Bridge Tuning — 2026-11-24

## Summary

Applied a controlled resource rebalance and removed blocking Kafka waits from the MQTT callback path. Recreated only Spark Worker, Spark Bronze/Silver/Gold, and MQTT-Kafka Bridge. PostgreSQL, Kafka, Mosquitto, Airflow, and their data volumes/checkpoints were not recreated or modified.

## 1. Host and Spark memory

Before tuning, the host had 0.69–0.82 GiB available in earlier samples and was at 94.8–95.6% RAM use. A later pre-change sample had 1.91 GiB available.

Changes:
- Local ignored `.env`: `SPARK_WORKER_MEMORY=2048m`, `SPARK_WORKER_CORES=3`.
- `.env.example` updated with the same defaults.
- Compose defaults updated to 2048 MiB and 3 worker cores.
- Spark Silver `spark.cores.max` reduced from 2 to 1. Bronze and Gold remain at 1 core each.
- This makes the three long-running Spark applications fit the worker's 3-core / 2048-MiB advertised budget without four concurrent executor slots competing for the constrained local runtime.

Verified after recreation:
- Spark Master reports one ALIVE worker: 3 cores, 2048 MiB.
- Three active Spark applications (Bronze, Silver, Gold), each granted one executor core.
- Worker memory allocation: 1536 / 2048 MiB.
- Spark Worker container memory: approximately 922 MiB, down from approximately 1.42 GiB before tuning.
- Host available memory: approximately 2.00 GiB, 87.3% used at the recorded sample.
- No WSL shutdown or Docker Desktop restart was performed.

## 2. Bronze/Silver latency

- Kept the existing Bronze 30-second trigger and its existing checkpoints.
- Kept the existing Silver 10-minute trigger and 200 shuffle partitions; no stateful checkpoint migration was attempted.
- Reduced Silver's executor concurrency to one core to fit the shared worker budget.
- After restart, Silver file listing warnings showed 78 files in 3.288 seconds and 280 files in 6.792 seconds, compared with earlier observations ranging from approximately 5 to 36 seconds.
- The earlier Bronze warnings were 33.697 and 33.330 seconds against a 30-second trigger. The post-change startup log window showed no matching trigger-overrun warning in the sampled period. This is an early improvement signal, not a completed long-duration soak test.

## 3. MQTT-to-Kafka bridge

The previous `on_message` callback synchronously waited up to 15 seconds for Kafka delivery. That blocked Paho's network loop and could delay MQTT polling.

Changes in `ingestion/mqtt/subscriber.py`:
- `on_message` now copies and enqueues messages into a bounded queue; it does not wait for Kafka.
- A dedicated daemon publisher thread performs Kafka send and delivery confirmation.
- MQTT QoS acknowledgements are sent only after Kafka confirms delivery.
- Kafka send failure leaves the message unacknowledged, disconnects MQTT, and discards queued unacknowledged work so the broker can redeliver.
- Queue saturation disconnects MQTT without acknowledging the message, allowing broker redelivery.
- Queue capacity is configurable via `MQTT_PUBLISH_QUEUE_SIZE`, default 500.

Compose and `.env.example` expose the queue-size setting.

Verification:
- Bridge reconnected to Kafka and MQTT after recreation.
- MQTT subscription to `egypt/air-quality/#` was confirmed in logs.
- No Event Loop Blocking warning appeared in the sampled post-change bridge logs.
- MQTT bridge tests cover successful forwarding, invalid payload pass-through, Kafka failure/no-ACK behavior, and queue saturation/backpressure.

No synthetic sensor payload was injected. Actual ESP32 payload receipt and end-to-end sensor acceptance remain unverified.

## 4. Container restarts / OOM

The prior Bronze and Gold containers had RestartCount=2 and their last inspected state showed OOMKilled=false. They were deliberately recreated to apply the new Spark configuration. The new Worker, Bronze, Silver, Gold, and Bridge containers all reported:
- RestartCount=0
- OOMKilled=false
- State=running

Recreation resets Docker's per-container restart counter; it does not prove the historical restart cause was fixed. The old Spark worker appears as DEAD in Spark Master history because it was replaced; the new worker is ALIVE. No data volume or checkpoint was deleted.

## 5. Validation

- Full test suite: 154 passed.
- MQTT bridge focused tests: 13 passed.
- `docker compose config -q`: passed.
- `git diff --check`: passed.
- All 16 Compose services were running after the Spark/bridge recreation.

## Remaining acceptance gates

1. Observe at least 30–60 minutes of normal pipeline load and confirm host memory remains above 1.5 GiB available.
2. Confirm Bronze trigger durations remain below 30 seconds over sustained operation, not only the initial sample.
3. Confirm Silver continues committing non-empty batches and Gold processes a new notification after the restart.
4. Validate a real ESP32/MQTT sensor payload from broker receipt through Kafka, Bronze, Silver, Gold, and PostgreSQL.
5. Continue monitoring bridge queue saturation and Kafka delivery failures.

## Conclusion

The immediate host-memory headroom improved to approximately 2 GiB, Spark is now scheduled within a 3-core / 2-GiB worker budget, and the MQTT callback no longer waits synchronously for Kafka. The local pipeline is running, but sustained performance and real sensor end-to-end acceptance are still required before Azure readiness sign-off.

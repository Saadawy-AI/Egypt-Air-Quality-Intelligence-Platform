import json
import logging
import os
from queue import Empty, Full, Queue
from threading import Event, Thread
import time

import paho.mqtt.client as mqtt
from kafka import KafkaProducer
from ingestion.config.kafka_security import kafka_python_security_config


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("MQTTKafkaBridge")


class MQTTKafkaBridge:
    def __init__(
        self,
        producer=None,
        mqtt_client=None,
        kafka_topic=None,
        topic_filter=None,
    ):
        self.kafka_topic = kafka_topic or os.getenv(
            "KAFKA_SENSOR_TOPIC",
            "air-quality",
        )
        self.topic_filter = topic_filter or os.getenv(
            "MQTT_TOPIC_FILTER",
            "egypt/air-quality/#",
        )
        self.producer = producer or KafkaProducer(
            bootstrap_servers=os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS",
                "kafka:29092",
            ).split(","),
            **kafka_python_security_config(),
            acks="all",
            retries=5,
            max_in_flight_requests_per_connection=1,
        )
        owns_mqtt_client = mqtt_client is None
        self.client = mqtt_client or mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=os.getenv(
                "MQTT_CLIENT_ID",
                "egypt-air-quality-kafka-bridge",
            ),
            clean_session=False,
        )
        if owns_mqtt_client:
            mqtt_username = os.getenv("MQTT_USERNAME")
            mqtt_password = os.getenv("MQTT_PASSWORD")
            if not mqtt_username or not mqtt_password:
                raise RuntimeError("MQTT_USERNAME and MQTT_PASSWORD are required for the local broker.")
            self.client.username_pw_set(mqtt_username, mqtt_password)
        self.client.manual_ack_set(True)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message

        queue_size = max(1, int(os.getenv("MQTT_PUBLISH_QUEUE_SIZE", "500")))
        self._publish_queue = Queue(maxsize=queue_size)
        self._stop_worker = Event()
        self._publisher_thread = Thread(
            target=self._publish_loop,
            name="mqtt-kafka-publisher",
            daemon=True,
        )
        self._publisher_thread.start()

    @staticmethod
    def extract_device_id(payload):
        try:
            record = json.loads(payload)
        except (TypeError, ValueError, json.JSONDecodeError):
            return None

        if not isinstance(record, dict):
            return None

        metadata = record.get("metadata")
        if isinstance(metadata, dict) and metadata.get("device_id"):
            return str(metadata["device_id"])
        if record.get("device_id"):
            return str(record["device_id"])
        return None

    def on_connect(self, client, userdata, flags, reason_code, properties):
        if getattr(reason_code, "is_failure", False):
            logger.error("MQTT connection rejected: %s", reason_code)
            return

        result, message_id = client.subscribe(self.topic_filter, qos=1)
        if result != mqtt.MQTT_ERR_SUCCESS:
            logger.error(
                "MQTT subscription failed | topic_filter=%s | rc=%s",
                self.topic_filter,
                result,
            )
            return
        logger.info("Subscribed to MQTT topic filter: %s", self.topic_filter)

    def on_disconnect(
        self,
        client,
        userdata,
        disconnect_flags,
        reason_code,
        properties,
    ):
        if getattr(reason_code, "is_failure", False):
            logger.warning("MQTT disconnected unexpectedly: %s", reason_code)
        else:
            logger.info("MQTT bridge disconnected.")

    def on_message(self, client, userdata, message):
        # Keep Paho's network-loop callback non-blocking. Kafka I/O and its
        # delivery wait happen on the dedicated publisher thread below.
        payload = message.payload.decode("utf-8", errors="replace")
        device_id = self.extract_device_id(payload)
        key = device_id.encode("utf-8") if device_id else None

        try:
            self._publish_queue.put_nowait(
                (
                    client,
                    message.topic,
                    message.mid,
                    message.qos,
                    key,
                    payload.encode("utf-8"),
                    device_id,
                )
            )
        except Full:
            # Never acknowledge a message we could not enqueue. Disconnecting
            # makes the QoS 1 broker redeliver it after the queue drains.
            logger.error(
                "MQTT publish queue is full; leaving message unacknowledged "
                "| mqtt_topic=%s | message_id=%s",
                message.topic,
                message.mid,
            )
            client.disconnect()

    def _publish_loop(self):
        while not self._stop_worker.is_set():
            try:
                item = self._publish_queue.get(timeout=0.25)
            except Empty:
                continue

            client, mqtt_topic, message_id, qos, key, payload, device_id = item
            try:
                self.producer.send(
                    self.kafka_topic,
                    key=key,
                    value=payload,
                ).get(timeout=15)
            except Exception:
                # Do not ACK on failure. Drain queued (also unacknowledged)
                # messages so broker redelivery remains the source of truth.
                logger.exception(
                    "Kafka publish failed; leaving MQTT message unacknowledged "
                    "| mqtt_topic=%s | message_id=%s",
                    mqtt_topic,
                    message_id,
                )
                client.disconnect()
                self._discard_queued_messages()
                self._publish_queue.task_done()
                continue

            if qos > 0:
                result = client.ack(message_id, qos)
                if result != mqtt.MQTT_ERR_SUCCESS:
                    logger.error(
                        "MQTT acknowledgement failed | message_id=%s | rc=%s",
                        message_id,
                        result,
                    )
                    client.disconnect()
                    self._publish_queue.task_done()
                    continue

            logger.info(
                "Forwarded MQTT measurement to Kafka | mqtt_topic=%s | "
                "kafka_topic=%s | device_id=%s",
                mqtt_topic,
                self.kafka_topic,
                device_id or "unknown",
            )
            self._publish_queue.task_done()

    def _discard_queued_messages(self):
        while True:
            try:
                self._publish_queue.get_nowait()
            except Empty:
                return
            else:
                self._publish_queue.task_done()

    def wait_for_idle(self, timeout=5):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self._publish_queue.unfinished_tasks == 0:
                return True
            time.sleep(0.01)
        return self._publish_queue.unfinished_tasks == 0

    def stop_publisher(self, timeout=16):
        self._stop_worker.set()
        self._publisher_thread.join(timeout=timeout)

    def run(self):
        host = os.getenv("MQTT_HOST", "mqtt")
        port = int(os.getenv("MQTT_PORT", "1883"))
        logger.info(
            "Starting MQTT-to-Kafka bridge | broker=%s:%s | kafka_topic=%s",
            host,
            port,
            self.kafka_topic,
        )
        self.client.connect_async(host, port, keepalive=60)
        try:
            self.client.loop_forever(retry_first_connection=True)
        finally:
            self.close()

    def close(self):
        self.stop_publisher()
        try:
            flush = getattr(self.producer, "flush", None)
            if flush:
                flush(timeout=15)
        finally:
            close = getattr(self.producer, "close", None)
            if close:
                close(timeout=15)


def main():
    MQTTKafkaBridge().run()


if __name__ == "__main__":
    main()

import json
import logging
import os

import paho.mqtt.client as mqtt
from kafka import KafkaProducer


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
            acks="all",
            retries=5,
            max_in_flight_requests_per_connection=1,
        )
        self.client = mqtt_client or mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=os.getenv(
                "MQTT_CLIENT_ID",
                "egypt-air-quality-kafka-bridge",
            ),
            clean_session=False,
        )
        self.client.manual_ack_set(True)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message

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
        payload = message.payload.decode("utf-8", errors="replace")
        device_id = self.extract_device_id(payload)
        key = device_id.encode("utf-8") if device_id else None

        try:
            self.producer.send(
                self.kafka_topic,
                key=key,
                value=payload.encode("utf-8"),
            ).get(timeout=15)
        except Exception:
            # With manual MQTT acknowledgements, the broker can redeliver this
            # QoS 1 message after reconnect instead of losing it on Kafka error.
            logger.exception(
                "Kafka publish failed; leaving MQTT message unacknowledged | "
                "mqtt_topic=%s | message_id=%s",
                message.topic,
                message.mid,
            )
            client.disconnect()
            return

        if message.qos > 0:
            result = client.ack(message.mid, message.qos)
            if result != mqtt.MQTT_ERR_SUCCESS:
                logger.error(
                    "MQTT acknowledgement failed | message_id=%s | rc=%s",
                    message.mid,
                    result,
                )
                client.disconnect()
                return

        logger.info(
            "Forwarded MQTT measurement to Kafka | mqtt_topic=%s | "
            "kafka_topic=%s | device_id=%s",
            message.topic,
            self.kafka_topic,
            device_id or "unknown",
        )

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
        try:
            self.producer.flush(timeout=15)
        finally:
            self.producer.close(timeout=15)


def main():
    MQTTKafkaBridge().run()


if __name__ == "__main__":
    main()

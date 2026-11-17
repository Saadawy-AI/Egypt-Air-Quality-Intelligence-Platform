import json
from threading import Event

from kafka import KafkaConsumer
from kafka.errors import KafkaError

from ingestion.config.settings import settings
from ingestion.utils.logger import get_logger


logger = get_logger("KafkaConsumer")


class KafkaConsumerClient:

    def __init__(
        self,
        bootstrap_servers=None,
        group_id="egypt-air-quality-consumer",
        topics=None,
        auto_offset_reset="earliest"
    ):

        self.bootstrap_servers = (
            bootstrap_servers
            or settings.KAFKA_BOOTSTRAP_SERVERS
        )

        self.group_id = group_id

        self.topics = topics or [
            settings.KAFKA_OPENAQ_TOPIC,
            settings.KAFKA_AIR_QUALITY_TOPIC,
            settings.KAFKA_WEATHER_TOPIC
        ]

        self.consumer = KafkaConsumer(

            bootstrap_servers=self.bootstrap_servers,

            group_id=self.group_id,

            enable_auto_commit=False,

            auto_offset_reset=auto_offset_reset,

            value_deserializer=lambda value: json.loads(
                value.decode("utf-8")
            ),

            key_deserializer=lambda key: (
                key.decode("utf-8") if key else None
            )
        )

        self.consumer.subscribe(self.topics)

        self._closed = False

        logger.info(
            "Kafka consumer initialized | group=%s | topics=%s",
            self.group_id,
            self.topics
        )

    # ==========================================
    # PROCESS SINGLE MESSAGE
    # ==========================================

    def consume_once(self, handler, timeout_ms=1000):

        if not callable(handler):
            raise TypeError("Handler must be callable.")

        if self._closed:
            raise RuntimeError("Consumer is already closed.")

        try:

            records = self.consumer.poll(
                timeout_ms=timeout_ms,
                max_records=1
            )

            if not records:
                return 0

            processed = 0

            for messages in records.values():

                for message in messages:

                    try:

                        handler(message.value)

                    except Exception:

                        logger.exception(
                            "Message processing failed | "
                            "topic=%s | partition=%s | offset=%s",
                            message.topic,
                            message.partition,
                            message.offset
                        )

                        raise

                    # Commit only after successful processing.
                    self.consumer.commit()

                    processed += 1

                    logger.info(
                        "Message processed and committed | "
                        "topic=%s | partition=%s | offset=%s",
                        message.topic,
                        message.partition,
                        message.offset
                    )

                    return processed

            return processed

        except KafkaError:

            logger.exception("Kafka consumer error.")
            raise

    # ==========================================
    # CONTINUOUS CONSUMPTION
    # ==========================================

    def consume_forever(
        self,
        handler,
        poll_timeout_ms=1000,
        stop_event=None
    ):

        if stop_event is None:
            stop_event = Event()

        logger.info("Kafka consumer started.")

        try:

            while not stop_event.is_set():

                self.consume_once(
                    handler=handler,
                    timeout_ms=poll_timeout_ms
                )

        except KeyboardInterrupt:

            logger.info("Kafka consumer interrupted.")

        finally:

            self.close()

    # ==========================================
    # CLOSE
    # ==========================================

    def close(self):

        if not self._closed:

            self.consumer.close(autocommit=False)

            self._closed = True

            logger.info("Kafka consumer closed.")

    # ==========================================
    # CONTEXT MANAGER
    # ==========================================

    def __enter__(self):

        return self

    def __exit__(self, exc_type, exc_value, traceback):

        self.close()

        return False

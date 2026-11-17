import json

from kafka import KafkaProducer
from kafka.errors import KafkaError

from ingestion.config.settings import settings
from ingestion.utils.logger import get_logger


logger = get_logger("KafkaProducer")


class KafkaProducerClient:

    def __init__(self, bootstrap_servers=None):

        self.bootstrap_servers = (
            bootstrap_servers
            or settings.KAFKA_BOOTSTRAP_SERVERS
        )

        self.producer = KafkaProducer(

            bootstrap_servers=self.bootstrap_servers,

            value_serializer=lambda value: json.dumps(
                value,
                ensure_ascii=False
            ).encode("utf-8"),

            key_serializer=lambda key: (
                key.encode("utf-8") if key else None
            ),

            acks="all",

            retries=5,

            max_in_flight_requests_per_connection=5,

            compression_type="gzip",

            linger_ms=10,

            request_timeout_ms=30000,

            max_block_ms=30000
        )

        logger.info(
            "Kafka producer initialized: %s",
            self.bootstrap_servers
        )

    # ==========================================
    # TOPIC ROUTING
    # ==========================================

    @staticmethod
    def get_topic(record):

        if not isinstance(record, dict):
            raise TypeError("Record must be a dictionary.")

        source = record.get("source")

        topics = {

            "openaq": settings.KAFKA_OPENAQ_TOPIC,

            "open_meteo_air_quality":
                settings.KAFKA_AIR_QUALITY_TOPIC,

            "open_meteo_weather":
                settings.KAFKA_WEATHER_TOPIC

        }

        topic = topics.get(source)

        if not topic:
            raise ValueError(
                f"Unsupported Kafka record source: {source}"
            )

        return topic

    # ==========================================
    # RECORD KEY
    # ==========================================

    @staticmethod
    def get_record_key(record):

        if not isinstance(record, dict):
            raise TypeError("Record must be a dictionary.")

        metadata = record.get("metadata")
        if not isinstance(metadata, dict):
            metadata = {}

        location = (
            record.get("governorate")
            or metadata.get("location_id")
            or "unknown"
        )

        return (
            f"{record.get('source')}:"
            f"{location}:"
            f"{record.get('timestamp')}"
        )

    # ==========================================
    # SEND SINGLE RECORD
    # ==========================================

    def send_record(self, record, topic=None):

        if not isinstance(record, dict):
            raise TypeError("Record must be a dictionary.")

        topic = topic or self.get_topic(record)

        key = self.get_record_key(record)

        try:

            future = self.producer.send(
                topic,
                value=record,
                key=key
            )

            metadata = future.get(timeout=30)

            logger.info(
                "Record delivered | topic=%s | partition=%s | offset=%s",
                metadata.topic,
                metadata.partition,
                metadata.offset
            )

            return metadata

        except KafkaError:

            logger.exception(
                "Failed to send record to topic=%s",
                topic
            )

            raise

    # ==========================================
    # SEND MULTIPLE RECORDS
    # ==========================================

    def send_records(self, records, topic=None):

        sent = 0

        for record in records:

            self.send_record(
                record,
                topic=topic
            )

            sent += 1

        logger.info(
            "Successfully delivered %s records.",
            sent
        )

        return sent

    # ==========================================
    # FLUSH
    # ==========================================

    def flush(self):

        self.producer.flush()

        logger.info("Kafka producer flushed.")

    # ==========================================
    # CLOSE
    # ==========================================

    def close(self):

        if self.producer:

            self.producer.flush()

            self.producer.close()

            logger.info("Kafka producer closed.")

    # ==========================================
    # CONTEXT MANAGER
    # ==========================================

    def __enter__(self):

        return self

    def __exit__(self, exc_type, exc_value, traceback):

        self.close()

        return False

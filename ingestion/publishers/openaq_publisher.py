from ingestion.config.settings import settings
from ingestion.producers.kafka_producer import KafkaProducerClient
from ingestion.utils.logger import get_logger


logger = get_logger("OpenAQPublisher")


class OpenAQPublisher:

    def __init__(self, producer_client=None):

        self.producer_client = (
            producer_client or KafkaProducerClient()
        )

        self.topic = settings.KAFKA_OPENAQ_TOPIC

        logger.info(
            "OpenAQ Publisher initialized | topic=%s",
            self.topic
        )

    # ==========================================
    # PUBLISH RECORDS
    # ==========================================

    def publish(self, records):
        """
        Publish validated OpenAQ records to Kafka.

        Returns:
            Dictionary containing publishing statistics.
        """

        records = list(records)

        total = len(records)
        sent = 0
        failures = []

        logger.info(
            "Starting OpenAQ publishing | total=%s",
            total
        )

        for index, record in enumerate(records):

            try:

                # Validate record type.
                if not isinstance(record, dict):

                    raise TypeError(
                        "Record must be a dictionary."
                    )

                # Ensure this publisher handles OpenAQ only.
                if record.get("source") != "openaq":

                    raise ValueError(
                        "OpenAQ publisher only accepts "
                        "records with source='openaq'."
                    )

                # Send and wait for Kafka acknowledgment.
                metadata = self.producer_client.send_record(
                    record,
                    topic=self.topic
                )

                sent += 1

                logger.info(
                    "OpenAQ record published | "
                    "index=%s topic=%s partition=%s offset=%s",
                    index,
                    metadata.topic,
                    metadata.partition,
                    metadata.offset
                )

            except Exception as exc:

                failure = {
                    "index": index,
                    "error": str(exc)
                }

                failures.append(failure)

                logger.exception(
                    "OpenAQ publishing failed | index=%s",
                    index
                )

                continue

        failed = total - sent

        success_rate = (
            round((sent / total) * 100, 2)
            if total > 0
            else 0.0
        )

        result = {
            "total": total,
            "sent": sent,
            "failed": failed,
            "success_rate": success_rate,
            "failures": failures
        }

        logger.info(
            "OpenAQ publishing completed | "
            "total=%s sent=%s failed=%s success_rate=%s%%",
            total,
            sent,
            failed,
            success_rate
        )

        return result

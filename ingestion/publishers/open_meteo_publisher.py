from ingestion.producers.kafka_producer import KafkaProducerClient
from ingestion.utils.logger import get_logger


logger = get_logger("OpenMeteoPublisher")


class OpenMeteoPublisher:

    SUPPORTED_SOURCES = {
        "open_meteo_air_quality",
        "open_meteo_weather"
    }

    def __init__(self, producer_client=None):

        self.producer_client = (
            producer_client or KafkaProducerClient()
        )

        logger.info(
            "Open-Meteo Publisher initialized | sources=%s",
            sorted(self.SUPPORTED_SOURCES)
        )

    # ==========================================
    # PUBLISH RECORDS
    # ==========================================

    def publish(self, records):
        """
        Publish validated Open-Meteo records to Kafka.

        Kafka topic is selected automatically
        by KafkaProducerClient based on record source.

        Returns:
            Dictionary containing publishing statistics.
        """

        records = list(records)

        total = len(records)
        sent = 0
        failures = []

        logger.info(
            "Starting Open-Meteo publishing | total=%s",
            total
        )

        for index, record in enumerate(records):

            try:

                # Validate record type.
                if not isinstance(record, dict):

                    raise TypeError(
                        "Record must be a dictionary."
                    )

                # Accept Open-Meteo sources only.
                source = record.get("source")

                if source not in self.SUPPORTED_SOURCES:

                    raise ValueError(
                        "Open-Meteo publisher only accepts "
                        "open_meteo_air_quality or "
                        "open_meteo_weather records."
                    )

                # Send using automatic topic routing.
                metadata = self.producer_client.send_record(
                    record
                )

                sent += 1

                logger.info(
                    "Open-Meteo record published | "
                    "index=%s source=%s topic=%s "
                    "partition=%s offset=%s",
                    index,
                    source,
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
                    "Open-Meteo publishing failed | index=%s",
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
            "Open-Meteo publishing completed | "
            "total=%s sent=%s failed=%s success_rate=%s%%",
            total,
            sent,
            failed,
            success_rate
        )

        return result

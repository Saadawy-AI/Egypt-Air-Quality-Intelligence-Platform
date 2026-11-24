from ingestion.sources.open_meteo_air_quality import (
    OpenMeteoAirQualityClient
)
from ingestion.sources.open_meteo_weather import (
    OpenMeteoWeatherClient
)
from ingestion.sources.openaq import OpenAQClient

from ingestion.pipelines.open_meteo_pipeline import (
    fetch_normalized_meteo_records
)
from ingestion.pipelines.openaq_pipeline import (
    fetch_normalized_latest_records
)

from ingestion.producers.kafka_producer import KafkaProducerClient

from ingestion.publishers.openaq_publisher import OpenAQPublisher
from ingestion.publishers.open_meteo_publisher import OpenMeteoPublisher

from ingestion.utils.logger import get_logger


logger = get_logger("IngestionMain")


class UnavailableSourceClient:
    """
    Adapter used when a source client cannot be initialized.
    Allows the pipeline to record the source failure
    without stopping other sources.
    """

    def __init__(self, source_name, error):
        self.source_name = source_name
        self.error = error

    def get_all_governorates(self):
        raise RuntimeError(
            f"{self.source_name} initialization failed: {self.error}"
        )


def safe_close(resource, resource_name):
    """
    Safely close a resource without affecting other cleanup operations.
    """

    if resource is None:
        return

    try:
        resource.close()
        logger.info("%s closed successfully.", resource_name)

    except Exception:
        logger.exception("Failed to close %s.", resource_name)


def run_ingestion():

    logger.info("=" * 60)
    logger.info("Starting Egypt Air Quality Ingestion")
    logger.info("=" * 60)

    # ==========================================
    # RESOURCE INITIALIZATION
    # ==========================================

    air_client = None
    weather_client = None
    openaq_client = None
    kafka_producer = None

    source_failures = []

    summary = {
        "openaq": {
            "valid": 0,
            "rejected": 0,
            "publishing": None
        },
        "open_meteo": {
            "valid": 0,
            "rejected": 0,
            "publishing": None
        }
    }

    try:

        # ==========================================
        # 1. INITIALIZE OPEN-METEO CLIENTS
        # ==========================================

        try:
            air_client = OpenMeteoAirQualityClient()
            logger.info("Open-Meteo Air Quality client initialized.")

        except Exception as error:
            logger.exception(
                "Could not initialize Open-Meteo Air Quality client."
            )

            air_client = UnavailableSourceClient(
                "open_meteo_air_quality",
                error
            )

        try:
            weather_client = OpenMeteoWeatherClient()
            logger.info("Open-Meteo Weather client initialized.")

        except Exception as error:
            logger.exception(
                "Could not initialize Open-Meteo Weather client."
            )

            weather_client = UnavailableSourceClient(
                "open_meteo_weather",
                error
            )

        # ==========================================
        # 2. INITIALIZE OPENAQ CLIENT
        # ==========================================

        try:
            openaq_client = OpenAQClient()
            logger.info("OpenAQ client initialized.")

        except Exception as error:
            source_failures.append(f"OpenAQ client initialization: {error}")
            logger.exception(
                "Could not initialize OpenAQ client. "
                "Continuing with other sources."
            )

        # ==========================================
        # 3. OPENAQ INGESTION
        # ==========================================

        openaq_records = []
        openaq_rejected = []

        if openaq_client is not None:

            try:
                logger.info("Fetching OpenAQ locations...")

                locations = (
                    openaq_client.get_all_egypt_locations()
                )

                logger.info(
                    "OpenAQ locations fetched: %s",
                    len(locations)
                )

                openaq_records, openaq_rejected = (
                    fetch_normalized_latest_records(
                        openaq_client,
                        locations
                    )
                )

            except Exception as error:
                source_failures.append(f"OpenAQ ingestion: {error}")
                logger.exception(
                    "OpenAQ ingestion failed. "
                    "Continuing with Open-Meteo."
                )

        else:
            logger.warning(
                "OpenAQ is unavailable. Skipping OpenAQ ingestion."
            )

        summary["openaq"]["valid"] = len(openaq_records)
        summary["openaq"]["rejected"] = len(openaq_rejected)

        # A location-level API failure is a source failure, not merely a
        # rejected measurement. Preserve valid records for at-least-once
        # publishing, then fail the Airflow task so the source is retried.
        for rejected in openaq_rejected:
            errors = rejected.get("errors", [])
            if any(str(error).startswith("Location processing failed:") for error in errors):
                source_failures.append(
                    "OpenAQ partial location failure: "
                    + "; ".join(str(error) for error in errors)
                )

        # ==========================================
        # 4. OPEN-METEO INGESTION
        # ==========================================

        meteo_records = []
        meteo_rejected = []

        try:
            (
                meteo_records,
                meteo_rejected
            ) = fetch_normalized_meteo_records(
                air_client,
                weather_client
            )

        except Exception as error:
            source_failures.append(f"Open-Meteo ingestion: {error}")
            logger.exception(
                "Open-Meteo ingestion failed unexpectedly."
            )

        summary["open_meteo"]["valid"] = len(meteo_records)
        summary["open_meteo"]["rejected"] = len(meteo_rejected)

        # Fetch failures are reported separately from record validation
        # rejects. A failed governorate/source must fail the DAG run even if
        # another Open-Meteo source returned publishable records.
        for rejected in meteo_rejected:
            source_name = rejected.get("source")
            errors = rejected.get("errors", [])
            if source_name in ("open_meteo_air_quality", "open_meteo_weather") and any(
                str(error).startswith("Fetch failed:") for error in errors
            ):
                source_failures.append(
                    f"{source_name} fetch failure: "
                    + "; ".join(str(error) for error in errors)
                )

        # ==========================================
        # 5. INGESTION SUMMARY
        # ==========================================

        total_valid = (
            len(openaq_records) +
            len(meteo_records)
        )

        total_rejected = (
            len(openaq_rejected) +
            len(meteo_rejected)
        )

        logger.info("=" * 60)
        logger.info("INGESTION SUMMARY")
        logger.info("OpenAQ valid records: %s", len(openaq_records))
        logger.info("OpenAQ rejected records: %s", len(openaq_rejected))
        logger.info("Open-Meteo valid records: %s", len(meteo_records))
        logger.info("Open-Meteo rejected records: %s", len(meteo_rejected))
        logger.info("Total valid records: %s", total_valid)
        logger.info("Total rejected records: %s", total_rejected)
        logger.info("=" * 60)

        # ==========================================
        # 6. KAFKA PUBLISHING
        # ==========================================

        if total_valid == 0:

            logger.warning(
                "No valid records available. Skipping Kafka publishing."
            )

        else:

            try:
                # One shared Kafka Producer for all publishers.
                kafka_producer = KafkaProducerClient()

                logger.info(
                    "Shared Kafka Producer initialized."
                )

            except Exception:
                logger.exception(
                    "Could not initialize Kafka Producer. "
                    "Records were not published."
                )

            if kafka_producer is not None:

                # ----------------------------------
                # OPENAQ PUBLISHING
                # ----------------------------------

                if openaq_records:

                    try:
                        openaq_publisher = OpenAQPublisher(
                            producer_client=kafka_producer
                        )

                        openaq_stats = openaq_publisher.publish(
                            openaq_records
                        )

                        summary["openaq"]["publishing"] = openaq_stats

                        logger.info(
                            "OpenAQ publishing results: %s",
                            openaq_stats
                        )

                    except Exception:
                        logger.exception(
                            "OpenAQ publishing failed."
                        )

                # ----------------------------------
                # OPEN-METEO PUBLISHING
                # ----------------------------------

                if meteo_records:

                    try:
                        meteo_publisher = OpenMeteoPublisher(
                            producer_client=kafka_producer
                        )

                        meteo_stats = meteo_publisher.publish(
                            meteo_records
                        )

                        summary["open_meteo"]["publishing"] = meteo_stats

                        logger.info(
                            "Open-Meteo publishing results: %s",
                            meteo_stats
                        )

                    except Exception:
                        logger.exception(
                            "Open-Meteo publishing failed."
                        )

    except Exception:
        logger.exception("Unexpected ingestion orchestration failure.")
        raise

    finally:

        # ==========================================
        # 7. RESOURCE CLEANUP
        # ==========================================

        logger.info("Starting resource cleanup...")

        safe_close(air_client, "Open-Meteo Air Quality client")
        safe_close(weather_client, "Open-Meteo Weather client")
        safe_close(openaq_client, "OpenAQ client")
        safe_close(kafka_producer, "Kafka Producer")

        logger.info("=" * 60)
        logger.info("Egypt Air Quality Ingestion Finished")
        logger.info("=" * 60)

    if source_failures:
        raise RuntimeError(
            "One or more ingestion sources failed: "
            + "; ".join(source_failures)
        )

    if total_valid == 0:
        raise RuntimeError(
            "Ingestion completed without any valid records."
        )

    for source_name, source_summary in summary.items():
        if source_summary["valid"] == 0:
            continue

        publishing = source_summary["publishing"]
        if publishing is None:
            raise RuntimeError(
                f"{source_name} records were not published to Kafka."
            )

        if publishing["failed"]:
            raise RuntimeError(
                f"{source_name} failed to publish "
                f"{publishing['failed']} records to Kafka."
            )

    return summary


if __name__ == "__main__":
    run_ingestion()

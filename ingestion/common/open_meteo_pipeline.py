from ingestion.processors.normalizer import (
    normalize_meteo_records
)

from ingestion.processors.validator import (
    get_validation_errors
)

from ingestion.utils.logger import get_logger


logger = get_logger("OpenMeteoPipeline")


def process_meteo_records(source_records):
    """
    Normalize and validate Open-Meteo source records.

    Args:
        source_records: Raw records returned by Open-Meteo clients.

    Returns:
        valid_records: List of validated normalized records.
        rejected_records: List of rejected records with errors.
    """

    valid_records = []
    rejected_records = []

    total_records = 0

    for source_record in source_records:

        source = source_record.get("source")

        logger.info(
            "Processing Open-Meteo source: %s | governorate=%s",
            source,
            source_record.get("governorate")
        )

        try:

            normalized_records = normalize_meteo_records(
                source_record
            )

        except Exception as exc:

            logger.exception(
                "Open-Meteo normalization failed | source=%s",
                source
            )

            rejected_records.append({
                "source_record": source_record,
                "errors": [
                    f"Normalization failed: {str(exc)}"
                ]
            })

            continue

        for record in normalized_records:

            total_records += 1

            errors = get_validation_errors(record)

            if errors:

                logger.warning(
                    "Invalid Open-Meteo record | "
                    "source=%s governorate=%s errors=%s",
                    source,
                    record.get("governorate"),
                    errors
                )

                rejected_records.append({
                    "record": record,
                    "errors": errors
                })

                continue

            valid_records.append(record)

    logger.info(
        "Open-Meteo processing completed | "
        "total=%s valid=%s rejected=%s",
        total_records,
        len(valid_records),
        len(rejected_records)
    )

    return valid_records, rejected_records


def fetch_normalized_meteo_records(
    air_quality_client,
    weather_client
):
    """
    Fetch Air Quality and Weather independently.

    Failure in one source will not stop the other source.

    Returns:
        valid_records, rejected_records
    """

    source_records = []
    fetch_failures = []

    # 1. Fetch Air Quality independently.
    try:
        air_quality_records = (
            air_quality_client.get_all_governorates()
        )

        source_records.extend(air_quality_records)

        logger.info(
            "Fetched Air Quality records: %s",
            len(air_quality_records)
        )

    except Exception as exc:
        logger.exception(
            "Failed to fetch Open-Meteo Air Quality"
        )

        fetch_failures.append({
            "source": "open_meteo_air_quality",
            "errors": [
                f"Fetch failed: {str(exc)}"
            ]
        })

    # 2. Fetch Weather independently.
    try:
        weather_records = (
            weather_client.get_all_governorates()
        )

        source_records.extend(weather_records)

        logger.info(
            "Fetched Weather records: %s",
            len(weather_records)
        )

    except Exception as exc:
        logger.exception(
            "Failed to fetch Open-Meteo Weather"
        )

        fetch_failures.append({
            "source": "open_meteo_weather",
            "errors": [
                f"Fetch failed: {str(exc)}"
            ]
        })

    # 3. Process all successfully fetched records.
    valid_records, rejected_records = (
        process_meteo_records(source_records)
    )

    rejected_records.extend(fetch_failures)

    logger.info(
        "Open-Meteo ingestion summary | "
        "valid=%s rejected=%s fetch_failures=%s",
        len(valid_records),
        len(rejected_records),
        len(fetch_failures)
    )

    return valid_records, rejected_records
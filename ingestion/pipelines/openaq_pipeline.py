from ingestion.processors.normalizer import (
    normalize_openaq_measurement
)

from ingestion.processors.validator import (
    get_validation_errors
)

from ingestion.utils.logger import get_logger


logger = get_logger("OpenAQPipeline")


def fetch_normalized_latest_records(client, locations):
    """
    Fetch latest OpenAQ measurements, match them
    with sensors, normalize and validate records.

    Each location is processed independently.
    A failure in one location will not stop others.
    """

    valid_records = []
    rejected_records = []
    total_measurements = 0

    for location in locations:

        location_id = location.get("id")

        if location_id is None:
            logger.warning(
                "Skipping location without ID: %s",
                location.get("name")
            )
            continue

        logger.info(
            "Processing OpenAQ location: %s",
            location_id
        )

        try:
            # Fetch sensors.
            sensors = client.get_sensors(location_id)

            sensor_map = {
                sensor.get("id"): sensor
                for sensor in sensors
                if sensor.get("id") is not None
            }

            # Fetch latest measurements.
            measurements = client.get_latest_measurements(
                location_id
            )

            logger.info(
                "Location %s: sensors=%s measurements=%s",
                location_id,
                len(sensor_map),
                len(measurements)
            )

            for measurement in measurements:

                total_measurements += 1

                sensor_id = measurement.get("sensorsId")
                sensor = sensor_map.get(sensor_id)

                if sensor is None:
                    logger.warning(
                        "Unknown sensor reference | "
                        "location=%s sensor=%s",
                        location_id,
                        sensor_id
                    )

                    rejected_records.append({
                        "measurement": measurement,
                        "errors": [
                            f"Unknown sensor reference: {sensor_id}"
                        ]
                    })
                    continue

                try:
                    record = normalize_openaq_measurement(
                        measurement=measurement,
                        location=location,
                        sensor=sensor
                    )

                    errors = get_validation_errors(record)

                    if errors:
                        logger.warning(
                            "Invalid OpenAQ record | "
                            "location=%s sensor=%s errors=%s",
                            location_id,
                            sensor_id,
                            errors
                        )

                        rejected_records.append({
                            "measurement": measurement,
                            "errors": errors
                        })
                        continue

                    valid_records.append(record)

                except Exception as exc:
                    logger.exception(
                        "Measurement processing failed | "
                        "location=%s sensor=%s",
                        location_id,
                        sensor_id
                    )

                    rejected_records.append({
                        "measurement": measurement,
                        "errors": [
                            f"Processing failed: {str(exc)}"
                        ]
                    })

        except Exception as exc:
            logger.exception(
                "OpenAQ location processing failed | location=%s",
                location_id
            )

            rejected_records.append({
                "location": location,
                "errors": [
                    f"Location processing failed: {str(exc)}"
                ]
            })

            continue

    logger.info(
        "OpenAQ processing completed | "
        "total=%s valid=%s rejected=%s",
        total_measurements,
        len(valid_records),
        len(rejected_records)
    )

    return valid_records, rejected_records
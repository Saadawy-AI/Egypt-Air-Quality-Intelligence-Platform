from math import asin, cos, radians, sin, sqrt

from ingestion.config.governorates import GOVERNORATES
from ingestion.processors.normalizer import (
    normalize_openaq_measurement
)

from ingestion.processors.validator import (
    get_validation_errors
)

from ingestion.utils.logger import get_logger


logger = get_logger("OpenAQPipeline")


def _governorate_for_location(location):
    """Resolve OpenAQ administrative labels, then fall back to nearest Egyptian governorate centroid.

    The coordinate fallback is deliberately bounded to avoid assigning distant
    or out-of-country stations to an unrelated governorate.
    """
    for key in ("governorate", "admin", "city", "locality", "name"):
        value = location.get(key)
        if not isinstance(value, str) or not value.strip():
            continue
        candidate = value.strip()
        for governorate in GOVERNORATES:
            if candidate.casefold() == governorate.casefold():
                return governorate

    coordinates = location.get("coordinates") or {}
    latitude = coordinates.get("latitude")
    longitude = coordinates.get("longitude")
    if latitude is None or longitude is None:
        return None
    try:
        latitude = float(latitude)
        longitude = float(longitude)
    except (TypeError, ValueError):
        return None
    if not (21.5 <= latitude <= 32.2 and 24.5 <= longitude <= 37.0):
        return None

    def distance_km(target_latitude, target_longitude):
        lat1, lat2 = radians(latitude), radians(target_latitude)
        delta_lat = lat2 - lat1
        delta_lon = radians(target_longitude - longitude)
        haversine = (
            sin(delta_lat / 2) ** 2
            + cos(lat1) * cos(lat2) * sin(delta_lon / 2) ** 2
        )
        return 6371.0 * 2 * asin(sqrt(haversine))

    governorate, distance = min(
        [
            (
                name,
                distance_km(details["latitude"], details["longitude"]),
            )
            for name, details in GOVERNORATES.items()
        ],
        key=lambda item: item[1],
    )
    if distance > 80:
        logger.warning(
            "OpenAQ location could not be assigned confidently | location_id=%s distance_km=%.1f",
            location.get("id"),
            distance,
        )
        return None
    logger.info(
        "Resolved OpenAQ governorate from coordinates | location_id=%s governorate=%s distance_km=%.1f",
        location.get("id"),
        governorate,
        distance,
    )
    return governorate


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
            governorate = _governorate_for_location(location)
            if governorate is None:
                logger.warning(
                    "OpenAQ location has no resolvable governorate | location_id=%s name=%s",
                    location_id,
                    location.get("name"),
                )

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
                        sensor=sensor,
                        governorate=governorate,
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
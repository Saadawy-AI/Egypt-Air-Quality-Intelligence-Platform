import math
from datetime import datetime

from ingestion.config.governorates import GOVERNORATES
from ingestion.processors.schema import (
    SCHEMA_VERSION,
    SUPPORTED_SOURCES
)


# Approximate geographic bounding box of Egypt.
EGYPT_BOUNDS = {
    "min_latitude": 21.5,
    "max_latitude": 32.2,
    "min_longitude": 24.5,
    "max_longitude": 37.0
}


# Known pollutant fields in Open-Meteo responses.
POLLUTANT_FIELDS = {
    "pm2_5",
    "pm10",
    "ozone",
    "nitrogen_dioxide",
    "sulphur_dioxide",
    "carbon_monoxide",
    "dust",
    "aerosol_optical_depth"
}


def is_valid_timestamp(value):
    """Check whether a timestamp is a valid ISO 8601 string."""

    if not isinstance(value, str) or not value.strip():
        return False

    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
        return True

    except ValueError:
        return False


def is_valid_number(value):
    """Check whether a value is a finite number."""

    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
    )


def validate_record(record):
    """
    Validate a unified ingestion record.

    Returns:
        bool: True if the record is valid, otherwise False.
    """

    return len(get_validation_errors(record)) == 0


def get_validation_errors(record):
    """
    Return all validation errors found in a record.

    Returns:
        list: List of validation error messages.
    """

    errors = []

    # 1. Record structure
    if not isinstance(record, dict):
        return ["Record must be a dictionary."]

    required_fields = [
        "schema_version",
        "source",
        "country_code",
        "latitude",
        "longitude",
        "timestamp",
        "ingested_at",
        "data",
        "metadata"
    ]

    for field in required_fields:
        if field not in record or record[field] is None:
            errors.append(f"Missing required field: {field}")

    if errors:
        return errors

    # 2. Schema version
    if record["schema_version"] != SCHEMA_VERSION:
        errors.append("Unsupported schema version.")

    # 3. Source validation
    if record["source"] not in SUPPORTED_SOURCES:
        errors.append("Unsupported data source.")

    # 4. Country validation
    if record["country_code"] != "EG":
        errors.append("Invalid country code. Expected EG.")

    # 5. Coordinate validation
    latitude = record["latitude"]
    longitude = record["longitude"]

    if not is_valid_number(latitude):
        errors.append("Latitude must be a finite number.")

    if not is_valid_number(longitude):
        errors.append("Longitude must be a finite number.")

    if (
        is_valid_number(latitude)
        and is_valid_number(longitude)
    ):
        if not (
            EGYPT_BOUNDS["min_latitude"]
            <= latitude
            <= EGYPT_BOUNDS["max_latitude"]
        ):
            errors.append("Latitude is outside Egypt's approximate bounds.")

        if not (
            EGYPT_BOUNDS["min_longitude"]
            <= longitude
            <= EGYPT_BOUNDS["max_longitude"]
        ):
            errors.append("Longitude is outside Egypt's approximate bounds.")

    # 6. Governorate validation
    governorate = record.get("governorate")

    if governorate is not None:
        if governorate not in GOVERNORATES:
            errors.append("Unknown governorate.")

        else:
            expected_arabic_name = GOVERNORATES[governorate]["name_ar"]
            governorate_ar = record.get("governorate_ar")

            if (
                governorate_ar is not None
                and governorate_ar != expected_arabic_name
            ):
                errors.append("Governorate Arabic name does not match.")

    # 7. Timestamp validation
    if not is_valid_timestamp(record["timestamp"]):
        errors.append("Invalid timestamp.")

    if not is_valid_timestamp(record["ingested_at"]):
        errors.append("Invalid ingestion timestamp.")

    # 8. Data validation
    data = record["data"]

    if not isinstance(data, dict) or not data:
        errors.append("Data must be a non-empty dictionary.")

    else:
        if record["source"] == "openaq":

            pollutant = data.get("pollutant")
            value = data.get("value")
            unit = data.get("unit")

            if not isinstance(pollutant, str) or not pollutant.strip():
                errors.append("OpenAQ pollutant name is invalid.")

            if not is_valid_number(value):
                errors.append("OpenAQ measurement value must be numeric.")

            elif value < 0:
                errors.append("OpenAQ pollutant value cannot be negative.")

            if not isinstance(unit, str) or not unit.strip():
                errors.append("OpenAQ measurement unit is invalid.")

        else:
            for field, value in data.items():

                if not is_valid_number(value):
                    errors.append(
                        f"Data field '{field}' must contain a finite number."
                    )

                elif field in POLLUTANT_FIELDS and value < 0:
                    errors.append(
                        f"Pollutant '{field}' cannot have a negative value."
                    )

    # 9. Metadata validation
    if not isinstance(record["metadata"], dict):
        errors.append("Metadata must be a dictionary.")

    return errors

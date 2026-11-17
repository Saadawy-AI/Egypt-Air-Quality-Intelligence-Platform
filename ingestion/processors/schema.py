from datetime import datetime, timezone


SCHEMA_VERSION = "1.0"

SUPPORTED_SOURCES = {
    "openaq",
    "open_meteo_air_quality",
    "open_meteo_weather"
}


def create_record(
    source,
    latitude,
    longitude,
    timestamp,
    data,
    country_code="EG",
    governorate=None,
    governorate_ar=None,
    metadata=None
):
    """
    Create a standardized ingestion record.
    """

    return {
        "schema_version": SCHEMA_VERSION,

        "source": source,

        "country_code": country_code,

        "governorate": governorate,

        "governorate_ar": governorate_ar,

        "latitude": latitude,

        "longitude": longitude,

        "timestamp": timestamp,

        "ingested_at": datetime.now(
            timezone.utc
        ).isoformat(),

        "data": data,

        "metadata": metadata or {}
    }

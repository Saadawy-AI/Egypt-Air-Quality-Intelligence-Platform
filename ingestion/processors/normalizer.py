from ingestion.config.governorates import GOVERNORATES
from ingestion.processors.schema import create_record


def normalize_openaq_measurement(
    measurement,
    location,
    sensor,
    governorate=None
):
    """
    Normalize both OpenAQ historical and latest measurements
    into the unified data schema.
    """

    coordinates = (
        measurement.get("coordinates")
        or location.get("coordinates")
        or {}
    )

    parameter = (
        measurement.get("parameter")
        or sensor.get("parameter")
        or {}
    )

    # Support historical and latest API response formats.
    period = measurement.get("period") or {}

    datetime_from = period.get("datetimeFrom") or {}

    datetime_info = (
        measurement.get("datetime")
        or datetime_from
        or {}
    )

    datetime_to = period.get("datetimeTo") or {}

    coverage = measurement.get("coverage") or {}

    flag_info = measurement.get("flagInfo") or {}

    value = measurement.get("value")

    data = {
        "pollutant": parameter.get("name"),
        "value": value,
        "unit": parameter.get("units")
    }

    metadata = {
        "location_id": location.get("id"),
        "location_name": location.get("name"),
        "locality": location.get("locality"),

        "sensor_id": sensor.get("id"),

        "datetime_local": datetime_info.get("local"),

        "datetime_to": datetime_to.get("utc"),

        "interval": period.get("interval"),

        "has_flags": flag_info.get("hasFlags"),

        "coverage_percent": coverage.get(
            "percentCoverage"
        ),

        "measurement_source": (
            "latest"
            if measurement.get("datetime")
            else "historical"
        )
    }

    governorate_ar = None

    if governorate in GOVERNORATES:
        governorate_ar = GOVERNORATES[governorate]["name_ar"]

    return create_record(
        source="openaq",

        latitude=coordinates.get("latitude"),
        longitude=coordinates.get("longitude"),

        timestamp=datetime_info.get("utc"),

        governorate=governorate,

        governorate_ar=governorate_ar,

        data=data,

        metadata=metadata
    )

def normalize_meteo_records(record):
    """
    Convert Open-Meteo hourly arrays
    into individual timestamp records.
    """

    hourly = record.get("data", {}).get("hourly", {})

    timestamps = hourly.get("time", [])

    if not isinstance(timestamps, list):
        return []

    results = []

    for index, timestamp in enumerate(timestamps):

        hourly_data = {}

        for variable, values in hourly.items():

            if variable == "time":
                continue

            if not isinstance(values, list):
                continue

            if index >= len(values):
                continue

            value = values[index]

            if value is not None:
                hourly_data[variable] = value

        if not hourly_data:
            continue

        normalized = create_record(
            source=record.get("source"),

            latitude=record.get("latitude"),
            longitude=record.get("longitude"),

            timestamp=timestamp,

            governorate=record.get("governorate"),

            governorate_ar=record.get("governorate_ar"),

            data=hourly_data
        )

        results.append(normalized)

    return results

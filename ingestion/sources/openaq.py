import requests

from ingestion.config.settings import settings
from ingestion.utils.logger import get_logger
from ingestion.utils.retry import retry


logger = get_logger("OpenAQ")


class OpenAQTransientError(requests.exceptions.RequestException):
    """Raised when OpenAQ returns a temporary HTTP error."""

    pass


class OpenAQClient:
    """
    Client for interacting with OpenAQ API v3.

    Responsibilities:
    - Authenticate using OpenAQ API Key.
    - Retrieve monitoring locations in Egypt.
    - Retrieve sensors for each location.
    - Retrieve measurements for each sensor.
    - Handle pagination and temporary API errors.
    """

    def __init__(self):

        if not settings.OPENAQ_API_KEY:
            raise ValueError(
                "Missing OPENAQ_API_KEY. Check your .env file."
            )

        self.base_url = settings.OPENAQ_BASE_URL.rstrip("/")

        self.session = requests.Session()

        self.session.headers.update({
            "X-API-Key": settings.OPENAQ_API_KEY,
            "Accept": "application/json"
        })

        self.timeout = settings.REQUEST_TIMEOUT
        self.limit = settings.OPENAQ_PAGE_LIMIT

        logger.info("OpenAQ client initialized successfully.")

    # ==========================================
    # HTTP REQUEST
    # ==========================================

    @retry(
        max_retries=3,
        backoff_factor=2,
        exceptions=(
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            OpenAQTransientError
        )
    )
    def _get(self, endpoint, params=None):
        """
        Execute an authenticated GET request.
        """

        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        response = self.session.get(
            url,
            params=params,
            timeout=self.timeout
        )

        if (
            response.status_code in (408, 429)
            or response.status_code >= 500
        ):
            raise OpenAQTransientError(
                f"Temporary HTTP error: {response.status_code}"
            )

        response.raise_for_status()

        try:
            return response.json()

        except ValueError as error:
            raise OpenAQTransientError(
                "OpenAQ returned an invalid JSON response."
            ) from error

    # ==========================================
    # PAGINATION
    # ==========================================

    def _get_all_pages(self, endpoint, params=None):
        """
        Retrieve all available pages from an OpenAQ endpoint.
        """

        results = []
        page = 1

        while True:

            query = dict(params or {})

            query.update({
                "page": page,
                "limit": self.limit
            })

            data = self._get(endpoint, query)

            page_results = data.get("results", [])

            if not page_results:
                break

            results.extend(page_results)

            logger.info(
                "OpenAQ endpoint=%s page=%s records=%s total=%s",
                endpoint,
                page,
                len(page_results),
                len(results)
            )

            if len(page_results) < self.limit:
                break

            page += 1

        logger.info(
            "Completed pagination for %s. Total records: %s",
            endpoint,
            len(results)
        )

        return results

    # ==========================================
    # LOCATIONS
    # ==========================================

    def get_all_egypt_locations(self):
        """
        Retrieve all OpenAQ monitoring locations in Egypt.
        """

        logger.info(
            "Fetching OpenAQ monitoring locations for Egypt."
        )

        locations = self._get_all_pages(
            "/locations",
            {
                "iso": settings.COUNTRY_CODE
            }
        )

        logger.info(
            "Successfully retrieved %s locations in Egypt.",
            len(locations)
        )

        return locations

    # ==========================================
    # LOCATION SENSORS
    # ==========================================

    def get_sensors(self, location_id):
        """
        Retrieve all sensors associated with a location.
        """

        logger.info(
            "Fetching sensors for location_id=%s",
            location_id
        )

        sensors = self._get_all_pages(
            f"/locations/{location_id}/sensors"
        )

        logger.info(
            "Retrieved %s sensors for location_id=%s",
            len(sensors),
            location_id
        )

        return sensors

    # ==========================================
    # SENSOR MEASUREMENTS
    # ==========================================

    def get_measurements(
        self,
        sensor_id,
        date_from=None,
        date_to=None
    ):
        """
        Retrieve measurements for a specific sensor.

        date_from and date_to should use ISO 8601 format.
        """

        params = {}

        if date_from:
            params["datetime_from"] = date_from

        if date_to:
            params["datetime_to"] = date_to

        logger.info(
            "Fetching measurements for sensor_id=%s",
            sensor_id
        )

        measurements = self._get_all_pages(
            f"/sensors/{sensor_id}/measurements",
            params
        )

        logger.info(
            "Retrieved %s measurements for sensor_id=%s",
            len(measurements),
            sensor_id
        )

        return measurements

    # ==========================================
    # LOCATION LATEST MEASUREMENTS
    # ==========================================

    def get_latest_measurements(self, location_id):
        """
        Retrieve the latest available measurements for a location.
        """

        logger.info(
            "Fetching latest measurements for location_id=%s",
            location_id
        )

        return self._get_all_pages(
            f"/locations/{location_id}/latest"
        )

    # ==========================================
    # LOCATION DETAILS
    # ==========================================

    def get_location_details(self, location_id):
        """
        Retrieve metadata for a specific location.
        """

        logger.info(
            "Fetching location details for location_id=%s",
            location_id
        )

        data = self._get(
            f"/locations/{location_id}"
        )

        results = data.get("results", [])

        return results[0] if results else None

    # ==========================================
    # FETCH ALL LOCATION SENSORS
    # ==========================================

    def fetch_all_sensors(self, locations):
        """
        Retrieve sensors for all provided locations.

        Returns a list containing location and sensor metadata.
        """

        all_sensors = []

        for location in locations:

            location_id = location.get("id")

            if location_id is None:
                logger.warning(
                    "Skipping location without ID: %s",
                    location.get("name")
                )
                continue

            try:

                sensors = self.get_sensors(location_id)

                for sensor in sensors:

                    all_sensors.append({
                        "location": location,
                        "sensor": sensor
                    })

            except requests.exceptions.RequestException:

                logger.exception(
                    "Failed to retrieve sensors for location_id=%s",
                    location_id
                )

                raise

        logger.info(
            "Total sensors retrieved across locations: %s",
            len(all_sensors)
        )

        return all_sensors

    # ==========================================
    # FETCH ALL LOCATION MEASUREMENTS
    # ==========================================

    def fetch_all_measurements(
        self,
        locations,
        date_from=None,
        date_to=None
    ):
        """
        Retrieve measurements from all sensors in all locations.

        Returns normalized location-sensor-measurement records.
        """

        all_records = []

        for location in locations:

            location_id = location.get("id")

            if location_id is None:
                continue

            sensors = self.get_sensors(location_id)

            for sensor in sensors:

                sensor_id = sensor.get("id")

                if sensor_id is None:
                    continue

                measurements = self.get_measurements(
                    sensor_id,
                    date_from=date_from,
                    date_to=date_to
                )

                for measurement in measurements:

                    parameter = (
                        measurement.get("parameter")
                        or sensor.get("parameter")
                        or {}
                    )

                    coordinates = location.get("coordinates") or {}

                    period = measurement.get("period") or {}

                    datetime_from = period.get("datetimeFrom") or {}
                    datetime_to = period.get("datetimeTo") or {}

                    coverage = measurement.get("coverage") or {}

                    flag_info = measurement.get("flagInfo") or {}

                    record = {
                        "source": "openaq",
                        "country_code": settings.COUNTRY_CODE,

                        "location_id": location_id,
                        "location_name": location.get("name"),
                        "locality": location.get("locality"),

                        "sensor_id": sensor_id,

                        "parameter": parameter.get("name"),
                        "value": measurement.get("value"),
                        "unit": parameter.get("units"),

                        "latitude": coordinates.get("latitude"),
                        "longitude": coordinates.get("longitude"),

                        "datetime": datetime_from.get("utc"),
                        "datetime_local": datetime_from.get("local"),
                        "datetime_to": datetime_to.get("utc"),

                        "interval": period.get("interval"),

                        "has_flags": flag_info.get("hasFlags"),

                        "coverage_percent": coverage.get(
                            "percentCoverage"
                        )
                    }

                    all_records.append(record)


        logger.info(
            "Total OpenAQ measurement records retrieved: %s",
            len(all_records)
        )

        return all_records

    # ==========================================
    # CLOSE SESSION
    # ==========================================

    def close(self):
        """
        Close HTTP session.
        """

        if self.session:

            self.session.close()

            logger.info(
                "OpenAQ HTTP session closed."
            )

    # ==========================================
    # CONTEXT MANAGER
    # ==========================================

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        return False

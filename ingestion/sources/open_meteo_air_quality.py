import requests

from ingestion.config.settings import settings
from ingestion.config.governorates import GOVERNORATES
from ingestion.utils.logger import get_logger
from ingestion.utils.retry import retry


logger = get_logger("OpenMeteoAirQuality")


class OpenMeteoAirQualityClient:

    def __init__(self):
        self.base_url = settings.OPEN_METEO_BASE_URL
        self.session = requests.Session()

    @retry(
        max_retries=3,
        backoff_factor=2,
        exceptions=(
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout
        )
    )
    def _get(self, params):

        response = self.session.get(
            self.base_url,
            params=params,
            timeout=settings.REQUEST_TIMEOUT
        )

        if response.status_code == 429 or response.status_code >= 500:
            raise requests.exceptions.ConnectionError(
                f"Open-Meteo temporary error: {response.status_code}"
            )

        response.raise_for_status()
        return response.json()

    def get_governorate_data(self, governorate):

        location = GOVERNORATES[governorate]

        params = {
            "latitude": location["latitude"],
            "longitude": location["longitude"],
            "hourly": ",".join(
                settings.OPEN_METEO_AIR_QUALITY_VARIABLES
            ),
            "timezone": settings.TIMEZONE,
            "forecast_days": 2
        }

        data = self._get(params)

        return {
            "source": "open_meteo_air_quality",
            "governorate": governorate,
            "governorate_ar": location["name_ar"],
            "latitude": location["latitude"],
            "longitude": location["longitude"],
            "data": data
        }

    def get_all_governorates(self):

        results = []

        for governorate in GOVERNORATES:

            try:
                result = self.get_governorate_data(governorate)
                results.append(result)

                logger.info(
                    "Air quality fetched for %s",
                    governorate
                )

            except requests.exceptions.RequestException:
                logger.exception(
                    "Failed to fetch air quality for %s",
                    governorate
                )

        return results

    def close(self):
        self.session.close()

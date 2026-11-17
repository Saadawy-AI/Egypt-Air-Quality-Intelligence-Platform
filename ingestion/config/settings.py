import os
from pathlib import Path
from dotenv import load_dotenv

from ingestion.config.governorates import GOVERNORATES


# ==========================================
# PROJECT PATHS
# ==========================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

load_dotenv(PROJECT_ROOT / ".env")


# ==========================================
# APPLICATION SETTINGS
# ==========================================

class Settings:

    PROJECT_NAME = "Egypt Air Quality Intelligence Platform"

    BASE_DIR = PROJECT_ROOT
    DATA_DIR = PROJECT_ROOT / "data"


    # ======================================
    # COUNTRY CONFIGURATION
    # ======================================

    COUNTRY_CODE = "EG"
    TIMEZONE = os.getenv("PROJECT_TIMEZONE", "Africa/Cairo")

    GOVERNORATES = GOVERNORATES


    # ======================================
    # OPENAQ CONFIGURATION
    # ======================================

    OPENAQ_API_KEY = os.getenv("OPENAQ_API_KEY")

    OPENAQ_BASE_URL = os.getenv(
        "OPENAQ_BASE_URL",
        "https://api.openaq.org/v3"
    )

    OPENAQ_PAGE_LIMIT = 1000


    # ======================================
    # OPEN-METEO CONFIGURATION
    # ======================================

    OPEN_METEO_BASE_URL = os.getenv(
        "OPEN_METEO_BASE_URL",
        "https://air-quality-api.open-meteo.com/v1/air-quality"
    )

    OPEN_METEO_WEATHER_URL = os.getenv(
        "OPEN_METEO_WEATHER_URL",
        "https://api.open-meteo.com/v1/forecast"
    )

    OPEN_METEO_AIR_QUALITY_VARIABLES = [
        "pm10",
        "pm2_5",
        "carbon_monoxide",
        "nitrogen_dioxide",
        "sulphur_dioxide",
        "ozone",
        "dust",
        "uv_index"
    ]

    OPEN_METEO_WEATHER_VARIABLES = [
        "temperature_2m",
        "relative_humidity_2m",
        "wind_speed_10m",
        "wind_direction_10m",
        "surface_pressure",
        "precipitation"
    ]


    # ======================================
    # KAFKA CONFIGURATION
    # ======================================

    KAFKA_BOOTSTRAP_SERVERS = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "kafka:29092"
    )

    KAFKA_AIR_QUALITY_TOPIC = os.getenv(
        "KAFKA_AIR_QUALITY_TOPIC",
        "air-quality"
    )

    KAFKA_WEATHER_TOPIC = os.getenv(
        "KAFKA_WEATHER_TOPIC",
        "weather"
    )

    KAFKA_OPENAQ_TOPIC = os.getenv(
        "KAFKA_OPENAQ_TOPIC",
        "openaq-measurements"
    )


    # ======================================
    # MQTT CONFIGURATION
    # ======================================

    MQTT_BROKER_HOST = os.getenv(
        "MQTT_BROKER_HOST",
        "mqtt"
    )

    MQTT_BROKER_PORT = int(
        os.getenv("MQTT_BROKER_PORT", "1883")
    )

    MQTT_AIR_QUALITY_TOPIC = os.getenv(
        "MQTT_AIR_QUALITY_TOPIC",
        "sensors/air-quality"
    )


    # ======================================
    # INGESTION CONFIGURATION
    # ======================================

    REQUEST_TIMEOUT = int(
        os.getenv("INGESTION_REQUEST_TIMEOUT", "30")
    )

    MAX_RETRIES = int(
        os.getenv("INGESTION_MAX_RETRIES", "3")
    )

    RETRY_BACKOFF = float(
        os.getenv("INGESTION_RETRY_BACKOFF", "2")
    )


    # ======================================
    # LOGGING CONFIGURATION
    # ======================================

    LOG_LEVEL = os.getenv(
        "INGESTION_LOG_LEVEL",
        "INFO"
    )


# ==========================================
# SETTINGS INSTANCE
# ==========================================

settings = Settings()

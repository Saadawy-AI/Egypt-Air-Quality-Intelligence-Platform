"""Regression tests: Silver/Gold must reject non-finite measurement values.

Spark orders NaN above every number and treats +Infinity as >= 0, so a bare
``cast(value as double) >= 0`` check accepted "NaN" and "Infinity" OpenAQ
readings. One NaN would then poison the weighted-average upsert in PostgreSQL.

These tests need pyspark and a JVM; they are skipped when either is missing
(e.g. the lightweight dev venv) and run inside the apache/spark image.
"""
import importlib.util
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")
if shutil.which("java") is None:
    pytest.skip("a JVM is required for Spark tests", allow_module_level=True)

from pyspark.sql import SparkSession  # noqa: E402

SPARK_DIR = Path(__file__).resolve().parents[3] / "spark"


def _load(name, relative):
    if str(SPARK_DIR) not in sys.path:
        sys.path.insert(0, str(SPARK_DIR))
    spec = importlib.util.spec_from_file_location(name, SPARK_DIR / relative)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("numeric-validation-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="module")
def silver():
    return _load("silver_ingestion_under_test", "silver/ingestion.py")


@pytest.fixture(scope="module")
def gold():
    return _load("gold_aggregation_under_test", "gold/aggregation.py")


def _record(source, data, topic, **metadata):
    return json.dumps({
        "schema_version": "1.0",
        "source": source,
        "country_code": "EG",
        "governorate": "Cairo",
        "latitude": 30.04,
        "longitude": 31.23,
        "timestamp": "2026-11-27T10:00:00Z",
        "data": data,
        "metadata": metadata,
    }), topic


def _bronze(spark, silver, records):
    now = datetime(2026, 11, 27, 10, 5, tzinfo=timezone.utc)
    rows = [
        (None, raw, topic, 0, offset, now, now, now.date())
        for offset, (raw, topic) in enumerate(records)
    ]
    return spark.createDataFrame(rows, silver.BRONZE_SCHEMA)


def _openaq(value):
    return _record(
        "openaq",
        {"pollutant": "pm25", "unit": "ug/m3", "value": value},
        "openaq-measurements",
        sensor_id="42",
    )


def _weather(value):
    return _record("open_meteo_weather", {"temperature_2m": value}, "weather")


def _validity(spark, silver, records):
    frame = silver.parse_bronze_records(_bronze(spark, silver, records))
    return {row["offset"]: row["_is_valid"] for row in frame.select("offset", "_is_valid").collect()}


def test_openaq_rejects_nan_and_infinity_but_keeps_valid_values(spark, silver):
    records = [
        _openaq("12.5"),      # 0 valid
        _openaq("0"),         # 1 valid boundary
        _openaq("NaN"),       # 2 must be rejected
        _openaq("Infinity"),  # 3 must be rejected
        _openaq("-1"),        # 4 negative
        _openaq("abc"),       # 5 non numeric
    ]
    assert _validity(spark, silver, records) == {
        0: True, 1: True, 2: False, 3: False, 4: False, 5: False,
    }


def test_meteo_rejects_infinity_but_keeps_negative_temperatures(spark, silver):
    records = [
        _weather("-5.5"),         # 0 valid: sub-zero temperature
        _weather("Infinity"),     # 1 rejected
        _weather("-Infinity"),    # 2 rejected
        _weather("NaN"),          # 3 rejected
    ]
    assert _validity(spark, silver, records) == {0: True, 1: False, 2: False, 3: False}


def test_quarantine_reason_is_reported_for_non_finite_values(spark, silver):
    from pyspark.sql.functions import col

    frame = silver.parse_bronze_records(
        _bronze(spark, silver, [_openaq("NaN"), _weather("Infinity")])
    ).withColumn("reason", silver.validation_errors())
    reasons = {r["offset"]: r["reason"] for r in frame.select("offset", "reason").collect()}
    assert "invalid OpenAQ measurement" in reasons[0]
    assert "non-numeric meteorological measurement" in reasons[1]
    assert frame.filter(col("_is_valid")).count() == 0


def test_gold_aggregation_drops_non_finite_values_from_existing_silver(spark, gold):
    ts = datetime(2026, 11, 27, 10, 15, tzinfo=timezone.utc)

    def row(value):
        return {
            "record_id": f"openaq:1:pm25:{value}",
            "source": "openaq",
            "governorate": "Cairo",
            "latitude": 30.04,
            "longitude": 31.23,
            "event_time": ts,
            "data": {"pollutant": "pm25", "unit": "ug/m3", "value": value},
        }

    silver_df = spark.createDataFrame(
        [row("10"), row("20"), row("NaN"), row("Infinity")], gold.SILVER_SCHEMA
    )
    (result,) = gold.aggregate_silver(silver_df).collect()
    assert result["measurement_count"] == 2
    assert result["average_value"] == pytest.approx(15.0)
    assert result["maximum_value"] == pytest.approx(20.0)

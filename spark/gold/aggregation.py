import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    explode,
    from_json,
    lit,
    map_entries,
    when,
    max as spark_max,
    min as spark_min,
    to_date,
    window,
)
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


logging.basicConfig(level=logging.INFO)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from kafka_security import with_kafka_security_options

logger = logging.getLogger("GoldAggregation")

FINITE_DOUBLE_MAX = sys.float_info.max

PROJECT_DATA = "/opt/project-data"
SILVER_BASE_PATH = os.path.join(PROJECT_DATA, "silver")
GOLD_PATH = os.path.join(PROJECT_DATA, "gold", "hourly_measurements_v4")
GOLD_DLQ_PATH = os.getenv("GOLD_DLQ_PATH", os.path.join(PROJECT_DATA, "gold", "dead_letter"))
CHECKPOINT_PATH = os.path.join(PROJECT_DATA, "checkpoints", "gold_notifications_v5")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
SILVER_NOTIFICATION_TOPIC = "silver-batch-notifications"
# Bind-mounted volumes (Docker Desktop on Windows) can show a committed Silver
# batch a few seconds after its Kafka notification; wait briefly before treating
# the notification as failed and sending it to the DLQ.
SILVER_COMMIT_WAIT_SECONDS = float(os.getenv("SILVER_COMMIT_WAIT_SECONDS", "30"))

DATASET_GOLD_BATCH_BASE = {
    "air_quality": 1_000_000_000,
    "openaq_measurements": 2_000_000_000,
    "weather": 3_000_000_000,
}

SILVER_SCHEMA = StructType([
    StructField("record_id", StringType()),
    StructField("schema_version", StringType()),
    StructField("source", StringType()),
    StructField("country_code", StringType()),
    StructField("governorate", StringType()),
    StructField("governorate_ar", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("timestamp", StringType()),
    StructField("event_time", TimestampType()),
    StructField("data", MapType(StringType(), StringType())),
    StructField("metadata", MapType(StringType(), StringType())),
    StructField("topic", StringType()),
    StructField("partition", IntegerType()),
    StructField("offset", LongType()),
    StructField("kafka_key", StringType()),
    StructField("kafka_timestamp", TimestampType()),
    StructField("ingestion_timestamp", TimestampType()),
    StructField("ingestion_date", DateType()),
    # New Silver batches are stored under batch_id=... Hive partitions.
    # Historical Silver files without this partition are read with null.
    StructField("batch_id", StringType()),
])

NOTIFICATION_SCHEMA = StructType([
    StructField("dataset", StringType(), nullable=False),
    StructField("silver_batch_id", LongType(), nullable=False),
    StructField("row_count", LongType(), nullable=False),
    StructField("silver_path", StringType(), nullable=False),
])


def create_spark_session():
    return (
        SparkSession.builder
        .appName("EgyptAirQuality-Gold-Aggregation")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )


def aggregate_silver(silver):
    openaq = (
        silver
        .filter(col("source") == "openaq")
        .select(
            "source",
            "governorate",
            "latitude",
            "longitude",
            "event_time",
            col("data").getItem("pollutant").alias("metric"),
            col("data").getItem("value").cast("double").alias("value"),
            col("data").getItem("unit").alias("unit"),
        )
    )

    meteo = (
        silver
        .filter(col("source").isin("open_meteo_air_quality", "open_meteo_weather"))
        .withColumn("_entry", explode(map_entries(col("data"))))
        .select(
            "source",
            "governorate",
            "latitude",
            "longitude",
            "event_time",
            col("_entry.key").alias("metric"),
            col("_entry.value").cast("double").alias("value"),
            lit(None).cast("string").alias("unit"),
        )
    )

    sensors = (
        silver
        .filter(col("source").isin("esp32_pm_sensor", "iot_sensor_simulator", "wokwi_esp32"))
        .withColumn("_entry", explode(map_entries(col("data"))))
        .select(
            "source", "governorate", "latitude", "longitude", "event_time",
            col("_entry.key").alias("metric"),
            col("_entry.value").cast("double").alias("value"),
            when(col("_entry.key").isin("pm1", "pm25", "pm10"), lit("ug/m3"))
                .otherwise(lit(None).cast("string")).alias("unit"),
        )
    )

    measurements = (
        openaq.unionByName(meteo).unionByName(sensors)
        .filter(
            col("metric").isNotNull()
            & col("value").between(-FINITE_DOUBLE_MAX, FINITE_DOUBLE_MAX)
            & col("event_time").isNotNull()
        )
    )

    hourly = (
        measurements
        .groupBy(
            window(col("event_time"), "1 hour").alias("_window"),
            "source",
            "governorate",
            "metric",
            "unit",
        )
        .agg(
            avg("value").alias("average_value"),
            spark_min("value").alias("minimum_value"),
            spark_max("value").alias("maximum_value"),
            count("*").alias("measurement_count"),
            avg("latitude").alias("latitude"),
            avg("longitude").alias("longitude"),
            # Coordinate averages ignore nulls; persist their own denominators
            # so PostgreSQL can merge partial batches without biased weights.
            count(col("latitude")).alias("latitude_count"),
            count(col("longitude")).alias("longitude_count"),
        )
        .select(
            col("_window.start").alias("window_start"),
            col("_window.end").alias("window_end"),
            to_date(col("_window.start")).alias("window_date"),
            "source",
            "governorate",
            "metric",
            "unit",
            "average_value",
            "minimum_value",
            "maximum_value",
            "measurement_count",
            "latitude",
            "longitude",
            "latitude_count",
            "longitude_count",
        )
    )
    return hourly


def wait_for_success_marker(batch_path, timeout=None, interval=2.0):
    """Return True once ``batch_path/_SUCCESS`` is visible, False after ``timeout`` seconds."""
    if timeout is None:
        timeout = SILVER_COMMIT_WAIT_SECONDS
    marker = os.path.join(batch_path, "_SUCCESS")
    deadline = time.monotonic() + timeout
    while True:
        if os.path.isfile(marker):
            return True
        if time.monotonic() >= deadline:
            return False
        time.sleep(interval)


def process_notification(event, stream_batch_id, spark):
    dataset = event["dataset"]
    silver_batch_id = int(event["silver_batch_id"])
    row_count = int(event["row_count"])
    if dataset not in DATASET_GOLD_BATCH_BASE:
        raise ValueError(f"Unknown Silver notification dataset: {dataset!r}")
    if silver_batch_id < 0 or silver_batch_id >= 1_000_000_000:
        raise ValueError(f"Invalid Silver batch id: {silver_batch_id}")
    if row_count <= 0:
        raise ValueError("Silver notification must describe a non-empty batch")

    silver_batch_path = os.path.join(
        SILVER_BASE_PATH,
        dataset,
        f"batch_id={silver_batch_id:020d}",
    )
    if event["silver_path"] != silver_batch_path:
        raise ValueError(
            "Silver notification path does not match its dataset/batch identity"
        )
    if not wait_for_success_marker(silver_batch_path):
        raise RuntimeError(
            "Silver notification arrived before its committed Parquet batch "
            f"was visible: {silver_batch_path}"
        )

    silver_batch = spark.read.schema(SILVER_SCHEMA).parquet(silver_batch_path)
    actual_silver_rows = silver_batch.count()
    if actual_silver_rows != row_count:
        raise RuntimeError(
            "Silver notification row-count mismatch: "
            f"dataset={dataset}, batch={silver_batch_id}, "
            f"notified={row_count}, actual={actual_silver_rows}"
        )
    batch_aggregates = aggregate_silver(
        silver_batch.dropDuplicates(["record_id"])
    )
    if batch_aggregates.isEmpty():
        logger.info(
            "Skipped Silver batch without aggregatable measurements | dataset=%s | batch=%s",
            dataset,
            silver_batch_id,
        )
        return

    # Stable IDs encode the Silver dataset and batch. Duplicate Kafka
    # notifications therefore overwrite the same Gold contribution.
    gold_batch_id = DATASET_GOLD_BATCH_BASE[dataset] + silver_batch_id
    gold_batch_path = os.path.join(
        GOLD_PATH,
        f"batch_id={gold_batch_id:020d}",
    )
    (
        batch_aggregates.write
        .mode("overwrite")
        .parquet(gold_batch_path)
    )
    logger.info(
        "Wrote notification-driven Gold batch | dataset=%s | silver_batch=%s "
        "| gold_batch=%s | source_rows=%s | path=%s",
        dataset,
        silver_batch_id,
        gold_batch_id,
        row_count,
        gold_batch_path,
    )


def write_gold_dead_letter(event, stream_batch_id, error):
    event_data = event.asDict(recursive=True) if hasattr(event, "asDict") else dict(event)
    dataset = str(event_data.get("dataset") or "unknown")
    silver_batch_id = str(event_data.get("silver_batch_id") or "unknown")
    safe_dataset = "".join(char for char in dataset if char.isalnum() or char in "_-")[:80] or "unknown"
    safe_batch_id = "".join(char for char in silver_batch_id if char.isalnum() or char in "_-")[:80] or "unknown"
    os.makedirs(GOLD_DLQ_PATH, exist_ok=True)
    filename = f"{int(stream_batch_id):020d}_{safe_dataset}_{safe_batch_id}.json"
    destination = os.path.join(GOLD_DLQ_PATH, filename)
    temporary = destination + f".tmp-{os.getpid()}"
    payload = {
        "stream_batch_id": int(stream_batch_id),
        "notification": event_data,
        "error_type": type(error).__name__,
        "error_message": str(error)[:2000],
        "failed_at_utc": datetime.now(timezone.utc).isoformat(),
        "replay_hint": "Fix the underlying Silver batch, then republish the notification to silver-batch-notifications.",
    }
    try:
        with open(temporary, "w", encoding="utf-8") as target:
            json.dump(payload, target, ensure_ascii=False, default=str)
            target.write("\n")
        os.replace(temporary, destination)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    logger.error(
        "Moved invalid Silver notification to durable Gold DLQ | dataset=%s | batch=%s | path=%s | error=%s",
        dataset, silver_batch_id, destination, error,
    )


def process_notifications(batch, stream_batch_id, spark):
    if batch.isEmpty():
        return

    events = (
        batch.select("dataset", "silver_batch_id", "row_count", "silver_path")
        .dropDuplicates(["dataset", "silver_batch_id"])
        .collect()
    )
    for event in events:
        try:
            process_notification(event, stream_batch_id, spark)
        except Exception as error:
            # Persist the poison notification before returning successfully so
            # Spark can advance its checkpoint without silently losing it.
            write_gold_dead_letter(event, stream_batch_id, error)


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    os.makedirs(GOLD_PATH, exist_ok=True)

    try:
        # Kafka carries compact Silver batch notifications. Gold reads only
        # newly committed batch paths instead of recursively listing every
        # historical Parquet file on each trigger.
        kafka_stream = (
            with_kafka_security_options(spark.readStream.format("kafka"))
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", SILVER_NOTIFICATION_TOPIC)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "true")
            .load()
        )
        notifications = (
            kafka_stream
            .select(
                from_json(
                    col("value").cast("string"),
                    NOTIFICATION_SCHEMA,
                ).alias("notification")
            )
            .select("notification.*")
        )

        query = (
            notifications.writeStream
            .outputMode("append")
            .foreachBatch(
                lambda batch, batch_id: process_notifications(
                    batch,
                    batch_id,
                    spark,
                )
            )
            .option("checkpointLocation", CHECKPOINT_PATH)
            .trigger(processingTime="1 minute")
            .start()
        )
        query.awaitTermination()
    except Exception:
        logger.exception("Gold aggregation failed.")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

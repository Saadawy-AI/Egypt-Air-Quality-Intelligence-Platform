import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    explode,
    from_json,
    lit,
    map_entries,
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
logger = logging.getLogger("GoldAggregation")

PROJECT_DATA = "/opt/project-data"
SILVER_BASE_PATH = os.path.join(PROJECT_DATA, "silver")
GOLD_PATH = os.path.join(PROJECT_DATA, "gold", "hourly_measurements_v4")
CHECKPOINT_PATH = os.path.join(PROJECT_DATA, "checkpoints", "gold_notifications_v5")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
SILVER_NOTIFICATION_TOPIC = "silver-batch-notifications"

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
        .filter(col("source") != "openaq")
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

    measurements = (
        openaq.unionByName(meteo)
        .filter(
            col("metric").isNotNull()
            & col("value").isNotNull()
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


def process_notifications(batch, stream_batch_id, spark):
    if batch.isEmpty():
        return

    events = (
        batch.select("dataset", "silver_batch_id", "row_count", "silver_path")
        .dropDuplicates(["dataset", "silver_batch_id"])
        .collect()
    )
    for event in events:
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
        if not os.path.isfile(os.path.join(silver_batch_path, "_SUCCESS")):
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
            continue

        # Stable IDs encode the Silver dataset and batch. Duplicate Kafka
        # notifications therefore overwrite the same Gold contribution, and
        # the PostgreSQL ledger cannot add it twice.
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


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    os.makedirs(GOLD_PATH, exist_ok=True)

    try:
        # Kafka carries compact Silver batch notifications. Gold reads only
        # newly committed batch paths instead of recursively listing every
        # historical Parquet file on each trigger.
        kafka_stream = (
            spark.readStream
            .format("kafka")
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

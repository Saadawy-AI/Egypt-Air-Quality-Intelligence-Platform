import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    current_timestamp,
    explode,
    expr,
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
GOLD_PATH = os.path.join(PROJECT_DATA, "gold", "hourly_measurements")
CURRENT_GOLD_PATH = os.path.join(PROJECT_DATA, "gold", "current_measurements")
CHECKPOINT_PATH = os.path.join(PROJECT_DATA, "checkpoints", "gold")

DATASETS = ("air_quality", "openaq_measurements", "weather")

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
])


def create_spark_session():
    return (
        SparkSession.builder
        .appName("EgyptAirQuality-Gold-Aggregation")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def aggregate_silver(silver, current_only=False):
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

    if current_only:
        measurements = measurements.filter(
            (col("event_time") >= current_timestamp() - expr("INTERVAL 72 HOURS"))
            & (col("event_time") <= current_timestamp())
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
        )
    )
    return hourly


def refresh_gold(batch, batch_id, spark):
    if batch.isEmpty():
        return

    silver_frames = [
        spark.read
        .schema(SILVER_SCHEMA)
        .parquet(os.path.join(SILVER_BASE_PATH, dataset))
        for dataset in DATASETS
    ]
    silver = silver_frames[0]
    for frame in silver_frames[1:]:
        silver = silver.unionByName(frame)

    (
        aggregate_silver(silver)
        .write
        .mode("overwrite")
        .partitionBy("window_date")
        .parquet(GOLD_PATH)
    )

    (
        aggregate_silver(silver, current_only=True)
        .write
        .mode("overwrite")
        .partitionBy("window_date")
        .parquet(CURRENT_GOLD_PATH)
    )

    logger.info(
        "Refreshed Historical and Current Gold aggregates | batch=%s",
        batch_id,
    )


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    os.makedirs(GOLD_PATH, exist_ok=True)
    for dataset in DATASETS:
        os.makedirs(os.path.join(SILVER_BASE_PATH, dataset), exist_ok=True)

    try:
        silver_streams = [
            spark.readStream
            .schema(SILVER_SCHEMA)
            .parquet(os.path.join(SILVER_BASE_PATH, dataset))
            for dataset in DATASETS
        ]
        combined = silver_streams[0]
        for stream in silver_streams[1:]:
            combined = combined.unionByName(stream)

        query = (
            combined.writeStream
            .outputMode("append")
            .foreachBatch(
                lambda batch, batch_id: refresh_gold(
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

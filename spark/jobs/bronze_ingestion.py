import os
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    to_date
)


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("BronzeIngestion")


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:29092"
)

BRONZE_BASE_PATH = "/opt/project-data/bronze"

CHECKPOINT_BASE_PATH = "/opt/project-data/checkpoints"

MAX_OFFSETS_PER_TRIGGER = int(
    os.getenv("MAX_OFFSETS_PER_TRIGGER", "500")
)

if MAX_OFFSETS_PER_TRIGGER <= 0:
    raise ValueError(
        "MAX_OFFSETS_PER_TRIGGER must be greater than zero"
    )


TOPICS = {
    "air-quality": "air_quality",
    "openaq-measurements": "openaq_measurements",
    "weather": "weather"
}


def create_spark_session():

    return (
        SparkSession.builder
        .appName("EgyptAirQuality-Bronze-Ingestion")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )


def create_stream(spark, topic, dataset_name):

    logger.info(
        "Starting Bronze stream | topic=%s | dataset=%s",
        topic,
        dataset_name
    )

    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            KAFKA_BOOTSTRAP_SERVERS
        )
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option(
            "maxOffsetsPerTrigger",
            MAX_OFFSETS_PER_TRIGGER
        )
        .load()
    )

    bronze_records = (
        kafka_stream.select(
            col("key").cast("string").alias("kafka_key"),
            col("value").cast("string").alias("raw_value"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp")
        )
        .withColumn(
            "ingestion_timestamp",
            current_timestamp()
        )
        .withColumn(
            "ingestion_date",
            to_date(col("ingestion_timestamp"))
        )
    )

    output_path = os.path.join(
        BRONZE_BASE_PATH,
        dataset_name
    )

    checkpoint_path = os.path.join(
        CHECKPOINT_BASE_PATH,
        dataset_name
    )

    query = (
        bronze_records.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", output_path)
        .option(
            "checkpointLocation",
            checkpoint_path
        )
        .partitionBy("ingestion_date")
        .trigger(processingTime="30 seconds")
        .start()
    )

    logger.info(
        "Bronze stream started | topic=%s | output=%s",
        topic,
        output_path
    )

    return query


def main():

    spark = create_spark_session()

    spark.sparkContext.setLogLevel("WARN")

    queries = []

    try:

        logger.info("=" * 60)
        logger.info("EGYPT AIR QUALITY - BRONZE INGESTION")
        logger.info("=" * 60)

        for topic, dataset_name in TOPICS.items():

            query = create_stream(
                spark,
                topic,
                dataset_name
            )

            queries.append(query)

        logger.info(
            "All %s Bronze streams started successfully.",
            len(queries)
        )

        spark.streams.awaitAnyTermination()

    except KeyboardInterrupt:

        logger.info("Bronze ingestion interrupted.")

    except Exception:

        logger.exception("Bronze ingestion failed.")
        raise

    finally:

        logger.info("Stopping Bronze streams...")

        for query in queries:

            if query.isActive:
                query.stop()

        spark.stop()


if __name__ == "__main__":
    main()

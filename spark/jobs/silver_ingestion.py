import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    count,
    concat_ws,
    expr,
    from_json,
    lit,
    to_timestamp,
    to_utc_timestamp,
    to_json,
    struct,
    when,
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
logger = logging.getLogger("SilverIngestion")

PROJECT_DATA = "/opt/project-data"
BRONZE_BASE_PATH = os.path.join(PROJECT_DATA, "bronze")
SILVER_BASE_PATH = os.path.join(PROJECT_DATA, "silver")
QUARANTINE_BASE_PATH = os.path.join(PROJECT_DATA, "quarantine")
CHECKPOINT_BASE_PATH = os.path.join(PROJECT_DATA, "checkpoints", "silver")

DATASETS = {
    "air-quality": "air_quality",
    "openaq-measurements": "openaq_measurements",
    "weather": "weather",
}

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
SILVER_NOTIFICATION_TOPIC = "silver-batch-notifications"

BRONZE_SCHEMA = StructType([
    StructField("kafka_key", StringType()),
    StructField("raw_value", StringType()),
    StructField("topic", StringType()),
    StructField("partition", IntegerType()),
    StructField("offset", LongType()),
    StructField("kafka_timestamp", TimestampType()),
    StructField("ingestion_timestamp", TimestampType()),
    StructField("ingestion_date", DateType()),
])

RECORD_SCHEMA = StructType([
    StructField("schema_version", StringType()),
    StructField("source", StringType()),
    StructField("country_code", StringType()),
    StructField("governorate", StringType()),
    StructField("governorate_ar", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("timestamp", StringType()),
    StructField("ingested_at", StringType()),
    StructField("data", MapType(StringType(), StringType())),
    StructField("metadata", MapType(StringType(), StringType())),
])


def create_spark_session():
    return (
        SparkSession.builder
        .appName("EgyptAirQuality-Silver-Ingestion")
        .config("spark.sql.session.timeZone", "UTC")
        # Stateful deduplication uses the existing checkpoint's 200 partitions.
        # AQE is not supported by streaming queries, so disable it explicitly.
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )


def parse_bronze_records(bronze_stream):
    parsed = (
        bronze_stream
        .withColumn("_record", from_json(col("raw_value"), RECORD_SCHEMA))
        .select("*", "_record.*")
    )

    parsed_timestamp = to_timestamp(col("timestamp"))
    has_timezone = col("timestamp").rlike(r"(Z|[+-]\d{2}:\d{2})$")
    event_time = when(
        has_timezone,
        parsed_timestamp,
    ).otherwise(
        to_utc_timestamp(parsed_timestamp, "Africa/Cairo")
    )

    parsed = parsed.withColumn("event_time", event_time)

    openaq_valid = (
        (col("source") == "openaq")
        & col("metadata").getItem("sensor_id").isNotNull()
        & col("data").getItem("pollutant").isNotNull()
        & (col("data").getItem("pollutant") != "")
        & col("data").getItem("unit").isNotNull()
        & (col("data").getItem("unit") != "")
        & col("data").getItem("value").cast("double").isNotNull()
        & (col("data").getItem("value").cast("double") >= 0)
    )

    meteo_values_valid = expr(
        """
        forall(
            map_values(data),
            value -> try_cast(value AS DOUBLE) IS NOT NULL
                AND NOT isnan(try_cast(value AS DOUBLE))
        )
        """
    )
    meteo_pollutants_nonnegative = expr(
        """
        forall(
            map_entries(data),
            entry -> entry.key NOT IN (
                'pm2_5', 'pm10', 'ozone', 'nitrogen_dioxide',
                'sulphur_dioxide', 'carbon_monoxide', 'dust'
            )
            OR try_cast(entry.value AS DOUBLE) >= 0
        )
        """
    )
    meteo_valid = (
        col("source").isin("open_meteo_air_quality", "open_meteo_weather")
        & meteo_values_valid
        & meteo_pollutants_nonnegative
    )

    sensor_pollutants_nonnegative = expr(
        """
        forall(
            map_entries(data),
            entry -> entry.key NOT IN ('pm1', 'pm25', 'pm10')
                OR try_cast(entry.value AS DOUBLE) >= 0
        )
        """
    )
    sensor_valid = (
        (col("source") == "esp32_pm_sensor")
        & col("metadata").getItem("device_id").isNotNull()
        & (
            col("data").getItem("pm1").isNotNull()
            | col("data").getItem("pm25").isNotNull()
            | col("data").getItem("pm10").isNotNull()
        )
        & meteo_values_valid
        & sensor_pollutants_nonnegative
    )

    source_matches_topic = (
        ((col("topic") == "air-quality") & col("source").isin("open_meteo_air_quality", "esp32_pm_sensor"))
        | ((col("topic") == "weather") & (col("source") == "open_meteo_weather"))
        | ((col("topic") == "openaq-measurements") & (col("source") == "openaq"))
    )

    record_valid = (
        (col("schema_version") == "1.0")
        & (col("country_code") == "EG")
        & source_matches_topic
        & col("latitude").between(21.5, 32.2)
        & col("longitude").between(24.5, 37.0)
        & col("event_time").isNotNull()
        & (col("data").isNotNull())
        & (expr("size(data)") > 0)
        & (openaq_valid | meteo_valid | sensor_valid)
    )

    parsed = parsed.withColumn("_is_valid", coalesce(record_valid, lit(False)))

    openaq_record_id = concat_ws(
        ":",
        col("source"),
        col("metadata").getItem("sensor_id"),
        col("data").getItem("pollutant"),
        col("timestamp"),
    )
    sensor_record_id = concat_ws(
        ":",
        col("source"),
        col("metadata").getItem("device_id"),
        col("timestamp"),
    )
    meteo_record_id = concat_ws(
        ":",
        col("source"),
        col("governorate"),
        col("timestamp"),
    )
    offset_record_id = concat_ws(
        ":",
        lit("invalid"),
        col("topic"),
        col("partition").cast("string"),
        col("offset").cast("string"),
    )

    return (
        parsed
        .withColumn(
            "record_id",
            when(~col("_is_valid"), offset_record_id)
            .when(col("source") == "openaq", openaq_record_id)
            .when(col("source") == "esp32_pm_sensor", sensor_record_id)
            .otherwise(meteo_record_id),
        )
        .withColumn(
            "dedupe_time",
            coalesce(col("event_time"), col("kafka_timestamp")),
        )
    )


def validation_errors():
    return expr(
        """
        concat_ws(
            '; ',
            CASE WHEN _record IS NULL THEN 'invalid JSON record' END,
            CASE WHEN schema_version IS NULL OR schema_version <> '1.0'
                THEN 'unsupported schema version' END,
            CASE WHEN country_code IS NULL OR country_code <> 'EG'
                THEN 'country must be EG' END,
            CASE WHEN source IS NULL OR topic IS NULL OR NOT (
                (topic = 'air-quality' AND source IN ('open_meteo_air_quality', 'esp32_pm_sensor'))
                OR (topic = 'weather' AND source = 'open_meteo_weather')
                OR (topic = 'openaq-measurements' AND source = 'openaq')
            ) THEN 'source does not match Kafka topic' END,
            CASE WHEN latitude IS NULL OR latitude < 21.5 OR latitude > 32.2
                THEN 'latitude outside Egypt bounds' END,
            CASE WHEN longitude IS NULL OR longitude < 24.5 OR longitude > 37.0
                THEN 'longitude outside Egypt bounds' END,
            CASE WHEN event_time IS NULL THEN 'invalid timestamp' END,
            CASE WHEN data IS NULL OR size(data) = 0 THEN 'empty measurement data' END,
            CASE WHEN source = 'openaq' AND (
                metadata['sensor_id'] IS NULL
                OR data['pollutant'] IS NULL OR trim(data['pollutant']) = ''
                OR data['unit'] IS NULL OR trim(data['unit']) = ''
                OR try_cast(data['value'] AS DOUBLE) IS NULL
                OR try_cast(data['value'] AS DOUBLE) < 0
            ) THEN 'invalid OpenAQ measurement' END,
            CASE WHEN source = 'esp32_pm_sensor' AND (
                metadata['device_id'] IS NULL
                OR NOT (
                    data['pm1'] IS NOT NULL
                    OR data['pm25'] IS NOT NULL
                    OR data['pm10'] IS NOT NULL
                )
                OR NOT forall(
                    map_entries(data),
                    entry -> entry.key NOT IN ('pm1', 'pm25', 'pm10')
                        OR try_cast(entry.value AS DOUBLE) >= 0
                )
            ) THEN 'invalid ESP32 particulate measurement' END,
            CASE WHEN source <> 'openaq' AND NOT forall(
                map_values(data),
                value -> try_cast(value AS DOUBLE) IS NOT NULL
                    AND NOT isnan(try_cast(value AS DOUBLE))
            ) THEN 'non-numeric meteorological measurement' END,
            CASE WHEN source IN ('open_meteo_air_quality', 'open_meteo_weather')
                AND NOT forall(
                    map_entries(data),
                    entry -> entry.key NOT IN (
                        'pm2_5', 'pm10', 'ozone', 'nitrogen_dioxide',
                        'sulphur_dioxide', 'carbon_monoxide', 'dust'
                    )
                    OR try_cast(entry.value AS DOUBLE) >= 0
                ) THEN 'negative pollutant measurement' END
        )
        """
    )


def publish_batch_notification(spark, dataset_name, batch_id, row_count, silver_batch_path):
    notification = spark.createDataFrame(
        [(dataset_name, int(batch_id), int(row_count), silver_batch_path)],
        ["dataset", "silver_batch_id", "row_count", "silver_path"],
    )
    payload = notification.select(
        concat_ws(
            ":",
            col("dataset"),
            col("silver_batch_id").cast("string"),
        ).alias("key"),
        to_json(
            struct("dataset", "silver_batch_id", "row_count", "silver_path")
        ).alias("value"),
    )
    (
        payload.write
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", SILVER_NOTIFICATION_TOPIC)
        .save()
    )
    logger.info(
        "Published Silver batch notification | dataset=%s | batch=%s | rows=%s",
        dataset_name,
        batch_id,
        row_count,
    )


def write_batch(batch, batch_id, dataset_name, spark):
    # Two output partitions avoid a single serialized Parquet writer while
    # keeping write concurrency bounded on the shared Spark worker.
    batch = batch.coalesce(2).cache()
    try:
        # Materialize the cached micro-batch once and avoid writing empty
        # Silver/Quarantine datasets on every idle or fully-deduplicated trigger.
        counts = batch.agg(
            count(lit(1)).alias("total_count"),
            count(when(col("_is_valid"), lit(1))).alias("valid_count"),
            count(when(~col("_is_valid"), lit(1))).alias("rejected_count"),
        ).first()

        if counts["total_count"] == 0:
            logger.info(
                "Skipped empty Silver batch | dataset=%s | batch=%s",
                dataset_name,
                batch_id,
            )
            return

        valid_records = batch.filter(col("_is_valid")).select(
            "record_id",
            "schema_version",
            "source",
            "country_code",
            "governorate",
            "governorate_ar",
            "latitude",
            "longitude",
            "timestamp",
            "event_time",
            "data",
            "metadata",
            "topic",
            "partition",
            "offset",
            "kafka_key",
            "kafka_timestamp",
            "ingestion_timestamp",
            "ingestion_date",
        )
        rejected_records = (
            batch
            .filter(~col("_is_valid"))
            .withColumn("validation_errors", validation_errors())
            .select(
                "raw_value",
                "topic",
                "partition",
                "offset",
                "kafka_timestamp",
                "ingestion_timestamp",
                "ingestion_date",
                "validation_errors",
            )
        )

        # Use a deterministic directory per Structured Streaming batch.
        # If Spark retries a batch after a partial write, overwrite that batch's
        # output instead of appending duplicate Parquet rows.
        batch_partition = f"batch_id={batch_id:020d}"
        silver_batch_path = os.path.join(
            SILVER_BASE_PATH,
            dataset_name,
            batch_partition,
        )
        quarantine_batch_path = os.path.join(
            QUARANTINE_BASE_PATH,
            dataset_name,
            batch_partition,
        )

        if counts["valid_count"]:
            (
                valid_records.write
                .mode("overwrite")
                .partitionBy("ingestion_date")
                .parquet(silver_batch_path)
            )
        else:
            logger.info(
                "No valid records to write | dataset=%s | batch=%s",
                dataset_name,
                batch_id,
            )

        if counts["rejected_count"]:
            (
                rejected_records.write
                .mode("overwrite")
                .partitionBy("ingestion_date")
                .parquet(quarantine_batch_path)
            )
        else:
            logger.info(
                "No rejected records to quarantine | dataset=%s | batch=%s",
                dataset_name,
                batch_id,
            )

        logger.info(
            "Processed Silver batch | dataset=%s | batch=%s | valid=%s | quarantined=%s",
            dataset_name,
            batch_id,
            counts["valid_count"],
            counts["rejected_count"],
        )
        if counts["valid_count"]:
            publish_batch_notification(
                spark,
                dataset_name,
                batch_id,
                counts["valid_count"],
                silver_batch_path,
            )
    finally:
        batch.unpersist()


def create_stream(spark, topic, dataset_name):
    bronze_path = os.path.join(BRONZE_BASE_PATH, dataset_name)
    os.makedirs(bronze_path, exist_ok=True)
    os.makedirs(os.path.join(SILVER_BASE_PATH, dataset_name), exist_ok=True)
    os.makedirs(os.path.join(QUARANTINE_BASE_PATH, dataset_name), exist_ok=True)

    bronze_stream = (
        spark.readStream
        .schema(BRONZE_SCHEMA)
        .option("pathGlobFilter", "part-*.parquet")
        .parquet(bronze_path)
    )
    records = parse_bronze_records(bronze_stream)

    query = (
        records
        .withWatermark("dedupe_time", "7 days")
        .dropDuplicatesWithinWatermark(["record_id"])
        .writeStream
        .outputMode("append")
        .foreachBatch(
            lambda batch, batch_id: write_batch(
                batch,
                batch_id,
                dataset_name,
                spark,
            )
        )
        .option(
            "checkpointLocation",
            os.path.join(CHECKPOINT_BASE_PATH, dataset_name),
        )
        # A 10-minute cadence leaves headroom for slower stateful 200-partition
        # dedupe/write cycles on the Windows-backed project volume. Bronze
        # continues polling Kafka every 30 seconds.
        .trigger(processingTime="10 minutes")
        .start()
    )

    logger.info("Started Silver stream | topic=%s | path=%s", topic, bronze_path)
    return query


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    queries = []

    try:
        for topic, dataset_name in DATASETS.items():
            queries.append(create_stream(spark, topic, dataset_name))

        spark.streams.awaitAnyTermination()
    except Exception:
        logger.exception("Silver ingestion failed.")
        raise
    finally:
        for query in queries:
            if query.isActive:
                query.stop()
        spark.stop()


if __name__ == "__main__":
    main()

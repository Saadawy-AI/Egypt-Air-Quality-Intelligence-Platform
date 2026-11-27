import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from kafka_security import with_kafka_security_options  # noqa: E402

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main():

    spark = (
        SparkSession.builder
        .appName("EgyptAirQuality-Kafka-Connection-Test")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("EGYPT AIR QUALITY - KAFKA CONNECTION TEST")
    print("=" * 60)

    kafka_stream = (
        with_kafka_security_options(spark.readStream.format("kafka"))
        .option("kafka.bootstrap.servers", "kafka:29092")
        .option("subscribe", "air-quality")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", "10")
        .load()
    )

    messages = kafka_stream.select(
        col("key").cast("string").alias("key"),
        col("value").cast("string").alias("message"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp")
    )

    query = (
        messages.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", False)
        .trigger(once=True)
        .start()
    )

    query.awaitTermination()

    print("SUCCESS: Spark read messages from Kafka!")

    spark.stop()


if __name__ == "__main__":
    main()

"""Kafka SASL/PLAIN-over-TLS option helpers for Spark's Kafka connector."""
import os


def with_kafka_security_options(builder):
    username = os.getenv("KAFKA_SASL_USERNAME")
    password = os.getenv("KAFKA_SASL_PASSWORD")
    if not username or not password:
        raise RuntimeError("KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD are required.")
    tls_password = os.getenv("KAFKA_TLS_STORE_PASSWORD")
    if not tls_password:
        raise RuntimeError("KAFKA_TLS_STORE_PASSWORD is required for Kafka TLS.")
    jaas = (
        "org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="{username}" password="{password}";'
    )
    return (
        builder
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", jaas)
        .option("kafka.ssl.truststore.location", os.getenv("KAFKA_SSL_TRUSTSTORE_LOCATION", "/etc/kafka/secrets/kafka.truststore.p12"))
        .option("kafka.ssl.truststore.password", tls_password)
        .option("kafka.ssl.truststore.type", "PKCS12")
        .option("kafka.ssl.endpoint.identification.algorithm", "https")
    )

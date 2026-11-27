"""Shared Kafka SASL/PLAIN-over-TLS client configuration."""
import os


def _credentials():
    username = os.getenv("KAFKA_SASL_USERNAME")
    password = os.getenv("KAFKA_SASL_PASSWORD")
    if not username and not password:
        # Keep unit tests and explicitly isolated local tooling usable without
        # Kafka; Docker Compose requires both values for the running platform.
        return None
    if not username or not password:
        raise RuntimeError("Both KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD are required.")
    return username, password


def kafka_python_security_config():
    credentials = _credentials()
    if credentials is None:
        return {}
    username, password = credentials
    return {
        "security_protocol": "SASL_SSL",
        "sasl_mechanism": "PLAIN",
        "sasl_plain_username": username,
        "sasl_plain_password": password,
        "ssl_cafile": os.getenv("KAFKA_SSL_CA_LOCATION", "/etc/kafka/secrets/kafka.crt"),
        "ssl_check_hostname": True,
    }


def spark_kafka_security_options():
    credentials = _credentials()
    if credentials is None:
        return {}
    username, password = credentials
    tls_password = os.getenv("KAFKA_TLS_STORE_PASSWORD")
    if not tls_password:
        raise RuntimeError("KAFKA_TLS_STORE_PASSWORD is required for Kafka TLS.")
    return {
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": (
            "org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="{username}" password="{password}";'
        ),
        "kafka.ssl.truststore.location": os.getenv(
            "KAFKA_SSL_TRUSTSTORE_LOCATION",
            "/etc/kafka/secrets/kafka.truststore.p12",
        ),
        "kafka.ssl.truststore.password": tls_password,
        "kafka.ssl.truststore.type": "PKCS12",
        "kafka.ssl.endpoint.identification.algorithm": "https",
    }

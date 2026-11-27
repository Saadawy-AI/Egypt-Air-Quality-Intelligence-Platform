import pytest

from unittest.mock import MagicMock, patch

from ingestion.consumers.kafka_consumer import (
    KafkaConsumerClient
)


@pytest.fixture
def consumer_client():

    with patch(
        "ingestion.consumers.kafka_consumer.KafkaConsumer"
    ) as mock_kafka:

        client = KafkaConsumerClient(
            bootstrap_servers="localhost:9092",
            group_id="test-group"
        )

        yield client, mock_kafka


# ==========================================
# INITIALIZATION
# ==========================================

def test_consumer_initialization(consumer_client):

    client, mock_kafka = consumer_client

    mock_kafka.assert_called_once()

    assert client.bootstrap_servers == "localhost:9092"

    assert client.group_id == "test-group"

    assert len(client.topics) == 3

    client.consumer.subscribe.assert_called_once_with(
        client.topics
    )


def test_manual_commit_enabled(consumer_client):

    client, mock_kafka = consumer_client

    kwargs = mock_kafka.call_args.kwargs

    assert kwargs["enable_auto_commit"] is False


def test_custom_topics():

    custom_topics = ["test-topic"]

    with patch(
        "ingestion.consumers.kafka_consumer.KafkaConsumer"
    ):

        client = KafkaConsumerClient(
            topics=custom_topics
        )

        assert client.topics == custom_topics

        client.consumer.subscribe.assert_called_once_with(
            custom_topics
        )


# ==========================================
# MESSAGE PROCESSING
# ==========================================

def test_consume_success(consumer_client):

    client, _ = consumer_client

    message = MagicMock()

    message.value = {"source": "openaq"}
    message.topic = "openaq-measurements"
    message.partition = 0
    message.offset = 10

    client.consumer.poll.return_value = {
        "partition": [message]
    }

    handler = MagicMock()

    result = client.consume_once(handler)

    assert result == 1

    handler.assert_called_once_with(message.value)

    client.consumer.commit.assert_called_once()


def test_consume_no_messages(consumer_client):

    client, _ = consumer_client

    client.consumer.poll.return_value = {}

    handler = MagicMock()

    result = client.consume_once(handler)

    assert result == 0

    handler.assert_not_called()

    client.consumer.commit.assert_not_called()


def test_no_commit_when_handler_fails(consumer_client):

    client, _ = consumer_client

    message = MagicMock()

    message.value = {"source": "openaq"}
    message.topic = "openaq-measurements"
    message.partition = 0
    message.offset = 10

    client.consumer.poll.return_value = {
        "partition": [message]
    }

    handler = MagicMock(
        side_effect=ValueError("Processing failed")
    )

    with pytest.raises(ValueError):

        client.consume_once(handler)

    client.consumer.commit.assert_not_called()


def test_invalid_handler(consumer_client):

    client, _ = consumer_client

    with pytest.raises(TypeError):

        client.consume_once("not_callable")


def test_poll_parameters(consumer_client):

    client, _ = consumer_client

    client.consumer.poll.return_value = {}

    client.consume_once(
        MagicMock(),
        timeout_ms=2000
    )

    client.consumer.poll.assert_called_once_with(
        timeout_ms=2000,
        max_records=1
    )


# ==========================================
# CLOSE
# ==========================================

def test_consumer_close(consumer_client):

    client, _ = consumer_client

    client.close()

    client.consumer.close.assert_called_once_with(
        autocommit=False
    )

    assert client._closed is True


def test_consumer_close_idempotent(consumer_client):

    client, _ = consumer_client

    client.close()
    client.close()

    client.consumer.close.assert_called_once()


def test_consume_after_close(consumer_client):

    client, _ = consumer_client

    client.close()

    with pytest.raises(RuntimeError):

        client.consume_once(MagicMock())


# ==========================================
# CONTEXT MANAGER
# ==========================================

def test_context_manager(consumer_client):

    client, _ = consumer_client

    with client as consumer:

        assert consumer is client

    assert client._closed is True

    client.consumer.close.assert_called_once()

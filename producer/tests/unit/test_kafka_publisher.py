from unittest.mock import Mock, patch

import pytest

from producer.src.publishers.kafka_publisher import KafkaPublisher, KafkaPublishError


def test_producer_uses_confluent_configuration_names() -> None:
    with patch("producer.src.publishers.kafka_publisher.Producer") as producer_class:
        KafkaPublisher("kafka:9092")

    config = producer_class.call_args.args[0]

    assert config["bootstrap.servers"] == "kafka:9092"
    assert config["client.id"] == "transitpulse-producer"


def test_publish_serializes_event_and_polls_callbacks() -> None:
    producer = Mock()
    publisher = KafkaPublisher("kafka:9092", producer=producer)
    event = {"event_id": "event-1"}

    publisher.publish("transit.test.v1", "vehicle-1", event)

    producer.produce.assert_called_once()
    arguments = producer.produce.call_args.kwargs
    assert arguments["topic"] == "transit.test.v1"
    assert arguments["key"] == b"vehicle-1"
    assert arguments["value"] == b'{"event_id":"event-1"}'
    assert arguments["on_delivery"] == publisher._on_delivery
    producer.poll.assert_called_once_with(0)


def test_flush_raises_recorded_delivery_error() -> None:
    producer = Mock()
    producer.flush.return_value = 0
    publisher = KafkaPublisher("kafka:9092", producer=producer)
    message = Mock()
    message.topic.return_value = "transit.test.v1"

    publisher._on_delivery(Mock(), message)

    with pytest.raises(KafkaPublishError, match="Kafka delivery failed"):
        publisher.flush()

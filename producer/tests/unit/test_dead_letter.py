from pathlib import Path
from unittest.mock import Mock

import pytest

from producer.src.dead_letter.dead_letter_event import build_dead_letter_event
from producer.src.dead_letter.dead_letter_handler import DeadLetterHandler
from producer.src.publishers.kafka_publisher import KafkaPublisher
from producer.src.schemas.schema_validator import EventValidationError, SchemaValidator

CONTRACT_DIRECTORY = Path(__file__).resolve().parents[3] / "contracts"


def test_build_dead_letter_event_matches_contract() -> None:
    validator = SchemaValidator(CONTRACT_DIRECTORY)
    original_event = {
        "event_id": "vehicle-1:100",
        "event_type": "vehicle_position",
        "schema_version": 1,
    }

    dead_letter_event = build_dead_letter_event(
        event=original_event,
        error=EventValidationError("payload is required"),
        original_topic="transit.vehicle_positions.v1",
        original_key="vehicle-1",
    )

    validator.validate(dead_letter_event)
    assert dead_letter_event["raw_payload"] == original_event
    assert dead_letter_event["correlation_id"] == "vehicle-1:100"


def test_handler_validates_and_publishes_dead_letter_event() -> None:
    validator = Mock(spec=SchemaValidator)
    publisher = Mock(spec=KafkaPublisher)
    handler = DeadLetterHandler(
        validator=validator,
        publisher=publisher,
        dlq_topic="transit.dead_letter.v1",
    )

    dead_letter_event = handler.handle(
        event={"event_id": "event-1", "schema_version": 1},
        error=EventValidationError("invalid event"),
        original_topic="transit.vehicle_positions.v1",
        original_key="vehicle-1",
    )

    validator.validate.assert_called_once_with(dead_letter_event)
    publisher.publish.assert_called_once_with(
        topic="transit.dead_letter.v1",
        key=dead_letter_event["event_id"],
        event=dead_letter_event,
    )


def test_handler_does_not_publish_invalid_dead_letter_event() -> None:
    validator = Mock(spec=SchemaValidator)
    validator.validate.side_effect = EventValidationError("invalid dead letter")
    publisher = Mock(spec=KafkaPublisher)
    handler = DeadLetterHandler(
        validator=validator,
        publisher=publisher,
        dlq_topic="transit.dead_letter.v1",
    )

    with pytest.raises(EventValidationError, match="invalid dead letter"):
        handler.handle(
            event={},
            error=EventValidationError("invalid event"),
            original_topic="transit.vehicle_positions.v1",
            original_key=None,
        )

    publisher.publish.assert_not_called()

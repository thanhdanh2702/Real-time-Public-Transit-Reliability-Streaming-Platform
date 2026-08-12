from typing import Any

from producer.src.dead_letter.dead_letter_event import build_dead_letter_event
from producer.src.publishers.kafka_publisher import KafkaPublisher
from producer.src.schemas.schema_validator import EventValidationError, SchemaValidator


class DeadLetterHandler:
    def __init__(
        self,
        validator: SchemaValidator,
        publisher: KafkaPublisher,
        dlq_topic: str,
    ) -> None:
        self._validator = validator
        self._publisher = publisher
        self._dlq_topic = dlq_topic

    def handle(
        self,
        event: dict[str, Any],
        error: EventValidationError,
        original_topic: str,
        original_key: str | None,
    ) -> dict[str, Any]:
        dead_letter_event = build_dead_letter_event(
            event=event,
            error=error,
            original_topic=original_topic,
            original_key=original_key,
        )

        self._validator.validate(dead_letter_event)

        self._publisher.publish(
            topic=self._dlq_topic,
            key=dead_letter_event["event_id"],
            event=dead_letter_event,
        )

        return dead_letter_event

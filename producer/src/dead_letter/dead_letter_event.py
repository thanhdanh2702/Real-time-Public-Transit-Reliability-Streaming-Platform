from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from producer.src.schemas.schema_validator import EventValidationError


def build_dead_letter_event(
    event: dict[str, Any],
    error: EventValidationError,
    original_topic: str,
    original_key: str | None,
) -> dict[str, Any]:
    schema_version = event.get("schema_version")
    correlation_id = event.get("event_id")

    return {
        "event_id": str(uuid4()),
        "event_type": "dead_letter",
        "schema_version": 1,
        "failed_at": datetime.now(UTC).isoformat(),
        "failure_stage": "schema_validation",
        "error_type": type(error).__name__,
        "error_message": str(error),
        "original_topic": original_topic,
        "original_partition": None,
        "original_offset": None,
        "original_key": original_key,
        "original_schema_version": schema_version if type(schema_version) is int else None,
        "raw_payload": event,
        "correlation_id": correlation_id if isinstance(correlation_id, str) else None,
    }

import json
from typing import Any

from confluent_kafka import KafkaError, KafkaException, Message, Producer


class KafkaPublishError(RuntimeError):
    """Khong the gui event vao Kafka."""


class KafkaPublisher:
    def __init__(
        self,
        bootstrap_servers: str,
        client_id: str = "transitpulse-producer",
        producer: Producer | None = None,
    ) -> None:
        self._producer = (
            producer
            if producer is not None
            else Producer(
                {
                    "bootstrap.servers": bootstrap_servers,
                    "client.id": client_id,
                    "acks": "all",
                    "enable.idempotence": True,
                    "compression.type": "zstd",
                    "linger.ms": 20,
                    "delivery.timeout.ms": 120_000,
                }
            )
        )
        self._delivery_errors: list[str] = []

    def publish(
        self,
        topic: str,
        key: str,
        event: dict[str, Any],
    ) -> None:
        try:
            value = json.dumps(
                event,
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise KafkaPublishError("Event cannot be serialized to JSON") from exc

        try:
            self._producer.produce(
                topic=topic,
                key=key.encode("utf-8"),
                value=value,
                on_delivery=self._on_delivery,
            )
            self._producer.poll(0)

        except (BufferError, KafkaException) as exc:
            raise KafkaPublishError(f"Could not enqueue event to topic {topic}") from exc

    def flush(self, timeout_seconds: float = 10.0) -> None:
        remaining = self._producer.flush(timeout_seconds)

        if remaining:
            raise KafkaPublishError(f"{remaining} Kafka messages were not delivered")

        if self._delivery_errors:
            errors = "; ".join(self._delivery_errors)
            self._delivery_errors.clear()

            raise KafkaPublishError(f"Kafka delivery failed: {errors}")

    def _on_delivery(
        self,
        error: KafkaError | None,
        message: Message,
    ) -> None:
        if error is None:
            return

        self._delivery_errors.append(f"topic={message.topic()} error={error}")

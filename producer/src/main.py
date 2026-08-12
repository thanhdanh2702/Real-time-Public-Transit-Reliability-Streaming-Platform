import os
import time
from pathlib import Path
from typing import Any

from producer.src.clients.mbta_client import MBTAClient
from producer.src.dead_letter.dead_letter_handler import DeadLetterHandler
from producer.src.parsers.alert_parser import parse_service_alerts
from producer.src.parsers.gtfs_realtime_parser import parse_feed
from producer.src.parsers.trip_update_parser import parse_trip_updates
from producer.src.parsers.vehicle_position_parser import parse_vehicle_positions
from producer.src.publishers.kafka_publisher import KafkaPublisher
from producer.src.schemas.schema_validator import EventValidationError, SchemaValidator

CONTRACT_DIRECTORY = Path(__file__).resolve().parents[2] / "contracts"

VEHICLE_TOPIC = "transit.vehicle_positions.v1"
TRIP_TOPIC = "transit.trip_updates.v1"
ALERT_TOPIC = "transit.service_alerts.v1"
DLQ_TOPIC = "transit.dead_letter.v1"

FEEDS = (
    ("vehicle_positions", parse_vehicle_positions, VEHICLE_TOPIC, "vehicle_id"),
    ("trip_updates", parse_trip_updates, TRIP_TOPIC, "trip_id"),
    ("alerts", parse_service_alerts, ALERT_TOPIC, "alert_id"),
)


def publish_events(
    events: list[dict[str, Any]],
    topic: str,
    key_field: str,
    validator: SchemaValidator,
    publisher: KafkaPublisher,
    dead_letter_handler: DeadLetterHandler,
) -> tuple[int, int]:
    published = 0
    rejected = 0

    for event in events:
        key_value = event.get(key_field)
        key = str(key_value) if key_value is not None else None

        try:
            validator.validate(event)
        except EventValidationError as error:
            dead_letter_handler.handle(
                event=event,
                error=error,
                original_topic=topic,
                original_key=key,
            )
            rejected += 1
            continue

        publisher.publish(
            topic=topic,
            key=str(event[key_field]),
            event=event,
        )
        published += 1

    return published, rejected


def run_once(
    client: MBTAClient,
    validator: SchemaValidator,
    publisher: KafkaPublisher,
    dead_letter_handler: DeadLetterHandler,
) -> tuple[int, int]:
    published_total = 0
    rejected_total = 0

    for feed_name, parser, topic, key_field in FEEDS:
        response = client.fetch_feed(feed_name)
        events = parser(
            parse_feed(response.payload),
            response.fetched_at,
        )

        published, rejected = publish_events(
            events=events,
            topic=topic,
            key_field=key_field,
            validator=validator,
            publisher=publisher,
            dead_letter_handler=dead_letter_handler,
        )
        published_total += published
        rejected_total += rejected

    return published_total, rejected_total


def main() -> None:
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    poll_interval = int(os.getenv("MBTA_POLL_INTERVAL_SECONDS", "30"))

    client = MBTAClient(timeout_seconds=30)
    validator = SchemaValidator(CONTRACT_DIRECTORY)
    publisher = KafkaPublisher(bootstrap_servers=bootstrap_servers)
    dead_letter_handler = DeadLetterHandler(
        validator=validator,
        publisher=publisher,
        dlq_topic=DLQ_TOPIC,
    )

    try:
        while True:
            published, rejected = run_once(
                client=client,
                validator=validator,
                publisher=publisher,
                dead_letter_handler=dead_letter_handler,
            )
            publisher.flush()

            print(f"Published events: {published}, DLQ events: {rejected}")
            time.sleep(poll_interval)
    finally:
        try:
            publisher.flush()
        finally:
            client.close()


if __name__ == "__main__":
    main()

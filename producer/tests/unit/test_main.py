from datetime import UTC, datetime
from unittest.mock import Mock, call, patch

import pytest

import producer.src.main as main_module
from producer.src.dead_letter.dead_letter_handler import DeadLetterHandler
from producer.src.main import (
    ALERT_TOPIC,
    TRIP_TOPIC,
    VEHICLE_TOPIC,
    publish_events,
    run_once,
)
from producer.src.publishers.kafka_publisher import KafkaPublisher
from producer.src.schemas.schema_validator import EventValidationError, SchemaValidator


def test_publish_events_sends_valid_event_to_main_topic() -> None:
    validator = Mock(spec=SchemaValidator)
    publisher = Mock(spec=KafkaPublisher)
    dead_letter_handler = Mock(spec=DeadLetterHandler)
    event = {"event_id": "event-1", "vehicle_id": "vehicle-1"}

    result = publish_events(
        events=[event],
        topic=VEHICLE_TOPIC,
        key_field="vehicle_id",
        validator=validator,
        publisher=publisher,
        dead_letter_handler=dead_letter_handler,
    )

    publisher.publish.assert_called_once_with(
        topic=VEHICLE_TOPIC,
        key="vehicle-1",
        event=event,
    )
    dead_letter_handler.handle.assert_not_called()
    assert result == (1, 0)


def test_publish_events_sends_invalid_event_to_dead_letter_handler() -> None:
    error = EventValidationError("invalid event")
    validator = Mock(spec=SchemaValidator)
    validator.validate.side_effect = error
    publisher = Mock(spec=KafkaPublisher)
    dead_letter_handler = Mock(spec=DeadLetterHandler)
    event = {"event_id": "event-1", "vehicle_id": "vehicle-1"}

    result = publish_events(
        events=[event],
        topic=VEHICLE_TOPIC,
        key_field="vehicle_id",
        validator=validator,
        publisher=publisher,
        dead_letter_handler=dead_letter_handler,
    )

    dead_letter_handler.handle.assert_called_once_with(
        event=event,
        error=error,
        original_topic=VEHICLE_TOPIC,
        original_key="vehicle-1",
    )
    publisher.publish.assert_not_called()
    assert result == (0, 1)


def test_run_once_fetches_parses_and_routes_all_feeds() -> None:
    fetched_at = datetime(2026, 8, 12, tzinfo=UTC)
    responses = [
        Mock(payload=b"vehicle", fetched_at=fetched_at),
        Mock(payload=b"trip", fetched_at=fetched_at),
        Mock(payload=b"alert", fetched_at=fetched_at),
    ]
    client = Mock()
    client.fetch_feed.side_effect = responses
    validator = Mock(spec=SchemaValidator)
    publisher = Mock(spec=KafkaPublisher)
    dead_letter_handler = Mock(spec=DeadLetterHandler)
    vehicle_parser = Mock(return_value=[{"v": 1}])
    trip_parser = Mock(return_value=[{"t": 1}])
    alert_parser = Mock(return_value=[{"a": 1}])

    with (
        patch.object(
            main_module,
            "FEEDS",
            (
                ("vehicle_positions", vehicle_parser, VEHICLE_TOPIC, "vehicle_id"),
                ("trip_updates", trip_parser, TRIP_TOPIC, "trip_id"),
                ("alerts", alert_parser, ALERT_TOPIC, "alert_id"),
            ),
        ),
        patch("producer.src.main.parse_feed", side_effect=["vf", "tf", "af"]),
        patch(
            "producer.src.main.publish_events",
            side_effect=[(1, 0), (1, 0), (0, 1)],
        ) as route,
    ):
        result = run_once(client, validator, publisher, dead_letter_handler)

    assert client.fetch_feed.call_args_list == [
        call("vehicle_positions"),
        call("trip_updates"),
        call("alerts"),
    ]
    assert [item.kwargs["topic"] for item in route.call_args_list] == [
        VEHICLE_TOPIC,
        TRIP_TOPIC,
        ALERT_TOPIC,
    ]
    assert result == (2, 1)


def test_run_once_uses_feed_configuration() -> None:
    response = Mock(payload=b"vehicle", fetched_at=datetime(2026, 8, 12, tzinfo=UTC))
    client = Mock()
    client.fetch_feed.return_value = response
    parser = Mock(return_value=[])
    validator = Mock(spec=SchemaValidator)
    publisher = Mock(spec=KafkaPublisher)
    dead_letter_handler = Mock(spec=DeadLetterHandler)

    with (
        patch.object(
            main_module,
            "FEEDS",
            (("vehicle_positions", parser, VEHICLE_TOPIC, "vehicle_id"),),
        ),
        patch("producer.src.main.parse_feed", return_value="feed"),
    ):
        run_once(client, validator, publisher, dead_letter_handler)

    client.fetch_feed.assert_called_once_with("vehicle_positions")
    parser.assert_called_once_with("feed", response.fetched_at)


def test_main_flushes_and_closes_resources() -> None:
    client = Mock()
    validator = Mock(spec=SchemaValidator)
    publisher = Mock(spec=KafkaPublisher)
    dead_letter_handler = Mock(spec=DeadLetterHandler)

    with (
        patch("producer.src.main.MBTAClient", return_value=client),
        patch("producer.src.main.SchemaValidator", return_value=validator),
        patch("producer.src.main.KafkaPublisher", return_value=publisher),
        patch("producer.src.main.DeadLetterHandler", return_value=dead_letter_handler),
        patch("producer.src.main.run_once", side_effect=[(3, 1), KeyboardInterrupt]),
        patch("producer.src.main.time.sleep") as sleep,
        pytest.raises(KeyboardInterrupt),
    ):
        main_module.main()

    sleep.assert_called_once_with(30)
    assert publisher.flush.call_count == 2
    client.close.assert_called_once_with()

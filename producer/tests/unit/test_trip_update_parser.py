from datetime import UTC, datetime
from pathlib import Path

from google.transit import gtfs_realtime_pb2

from producer.src.parsers.trip_update_parser import parse_trip_updates
from producer.src.schemas.schema_validator import SchemaValidator

CONTRACT_DIRECTORY = Path(__file__).resolve().parents[3] / "contracts"
INGESTED_AT = datetime(2026, 8, 12, 3, 0, tzinfo=UTC)
FEED_TIMESTAMP = 1_700_000_000


def _new_feed() -> gtfs_realtime_pb2.FeedMessage:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = FEED_TIMESTAMP
    return feed


def test_parse_trip_updates_creates_one_event_with_all_stops() -> None:
    feed = _new_feed()
    entity = feed.entity.add()
    entity.id = "trip-update-1"

    trip_update = entity.trip_update
    trip_update.timestamp = FEED_TIMESTAMP + 10
    trip_update.delay = 90
    trip_update.trip.trip_id = "trip-1"
    trip_update.trip.route_id = "1"
    trip_update.trip.direction_id = 0
    trip_update.trip.schedule_relationship = gtfs_realtime_pb2.TripDescriptor.SCHEDULED
    trip_update.vehicle.id = "vehicle-1"

    first_stop = trip_update.stop_time_update.add()
    first_stop.stop_id = "stop-1"
    first_stop.stop_sequence = 1
    first_stop.arrival.time = FEED_TIMESTAMP + 300
    first_stop.arrival.delay = 120
    first_stop.departure.time = FEED_TIMESTAMP + 330
    first_stop.schedule_relationship = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.SCHEDULED

    second_stop = trip_update.stop_time_update.add()
    second_stop.stop_id = "stop-2"
    second_stop.stop_sequence = 2
    second_stop.arrival.time = FEED_TIMESTAMP + 600
    second_stop.departure.time = FEED_TIMESTAMP + 630
    second_stop.departure.delay = 180

    events = parse_trip_updates(feed, INGESTED_AT)

    assert len(events) == 1

    event = events[0]
    assert event["event_id"] == f"trip-update-1:{FEED_TIMESTAMP + 10}"
    assert event["trip_id"] == "trip-1"
    assert event["route_id"] == "1"
    assert event["vehicle_id"] == "vehicle-1"
    assert event["payload"]["trip_delay_seconds"] == 90
    assert len(event["payload"]["stop_time_updates"]) == 2

    first_event_stop, second_event_stop = event["payload"]["stop_time_updates"]
    assert first_event_stop["stop_id"] == "stop-1"
    assert first_event_stop["delay_seconds"] == 120
    assert first_event_stop["schedule_relationship"] == "SCHEDULED"
    assert second_event_stop["stop_id"] == "stop-2"
    assert second_event_stop["delay_seconds"] == 180

    SchemaValidator(CONTRACT_DIRECTORY).validate(event)


def test_parse_trip_updates_keeps_canceled_trip_without_stops() -> None:
    feed = _new_feed()
    entity = feed.entity.add()
    entity.id = "canceled-trip-update"
    entity.trip_update.trip.trip_id = "canceled-trip"
    entity.trip_update.trip.route_id = "66"
    entity.trip_update.trip.schedule_relationship = gtfs_realtime_pb2.TripDescriptor.CANCELED

    events = parse_trip_updates(feed, INGESTED_AT)

    assert len(events) == 1

    event = events[0]
    assert event["event_id"] == f"canceled-trip-update:{FEED_TIMESTAMP}"
    assert event["payload"]["schedule_relationship"] == "CANCELED"
    assert event["payload"]["stop_time_updates"] == []

    SchemaValidator(CONTRACT_DIRECTORY).validate(event)

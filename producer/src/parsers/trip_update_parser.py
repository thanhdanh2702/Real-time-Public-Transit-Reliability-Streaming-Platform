from datetime import UTC, datetime
from typing import Any

from google.transit import gtfs_realtime_pb2

def _to_iso(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp, UTC).isoformat()

def _stop_time_to_iso(
        event: gtfs_realtime_pb2.TripUpdate.StopTimeEvent
) -> str | None:
    if not event.HasField("time"):
        return None

    return _to_iso(event.time)

def parse_trip_updates(
        feed: gtfs_realtime_pb2.FeedMessage,
        ingested_at: datetime
) -> list[dict[str, Any]]:
    if not feed.header.HasField("timestamp"):
        raise ValueError("Feed header is missing timestamp field")

    feed_timestamp = feed.header.timestamp
    ingested_at_iso = ingested_at.astimezone(UTC).isoformat()
    published_at_iso = datetime.now(UTC).isoformat()

    events: list[dict[str, Any]] = []

    for entity in feed.entity:
        if entity.is_deleted or not entity.HasField("trip_update"):
            continue

        trip_update = entity.trip_update
        trip = trip_update.trip

        if not trip.trip_id:
            continue

        source_timestamp = (
            trip_update.timestamp
            if trip_update.HasField("timestamp")
            else feed_timestamp
        )

        for stop_update in trip_update.stop_time_update:
            if (
                not stop_update.stop_id
                or not stop_update.HasField("stop_sequence")
            ):
                continue

            delay_seconds = None

            if stop_update.arrival.HasField("delay"):
                delay_seconds = stop_update.arrival.delay
            elif stop_update.departure.HasField("delay"):
                delay_seconds = stop_update.departure.delay
            elif trip_update.HasField("delay"):
                delay_seconds = trip_update.delay

            event = {
                "event_id": (
                    f"{entity.id}:"
                    f"{stop_update.stop_sequence}:"
                    f"{source_timestamp}"
                ),
                "event_type": "trip_update",
                "schema_version": 1,
                "source": "mbta_gtfs_realtime",
                "source_timestamp": _to_iso(source_timestamp),
                "feed_timestamp":_to_iso(feed_timestamp),
                "ingested_at":ingested_at_iso,
                "published_at":published_at_iso,
                "route_id": trip.route_id or None,
                "trip_id":trip.trip_id,
                "vehicle_id": trip_update.vehicle.id or None,
                "payload": {
                    "direction_id": (
                        trip.direction_id
                        if trip.HasField("direction_id")
                        else None
                    ),
                    "schedule_relationship": (
                        gtfs_realtime_pb2.TripDescriptor.ScheduleRelationship.Name(
                            trip.schedule_relationship
                        )
                        if trip.HasField("schedule_relationship")
                        else None
                    ),
                    "stop_id": stop_update.stop_id,
                    "stop_sequence": stop_update.stop_sequence,
                    "predicted_arrival": _stop_time_to_iso(stop_update.arrival),
                    "predicted_departure": _stop_time_to_iso(stop_update.departure),
                    "delay_seconds": delay_seconds,
                    "stop_schedule_relationship": (
                        gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.ScheduleRelationship.Name(
                            stop_update.schedule_relationship
                        )
                        if stop_update.HasField("schedule_relationship")
                        else None
                    )
                }
            }

            events.append(event)

    return events
from datetime import UTC, datetime
from typing import Any

from google.transit import gtfs_realtime_pb2

def _to_iso(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp, UTC).isoformat()

def parse_vehicle_positions(
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
        if entity.is_deleted or not entity.HasField("vehicle"):
            continue

        vehicle = entity.vehicle

        if not vehicle.HasField("position"):
            continue

        position = vehicle.position
        trip = vehicle.trip
        description = vehicle.vehicle

        vehicle_id  = description.id or entity.id
        source_timestamp = (
            vehicle.timestamp
            if vehicle.HasField("timestamp")
            else feed_timestamp
        )

        event = {
            "event_id": f"{entity.id}:{source_timestamp}",
            "event_type": "vehicle_position",
            "schema_version": 1,
            "source": "mbta_gtfs_realtime",
            "source_timestamp": _to_iso(source_timestamp),
            "feed_timestamp": _to_iso(feed_timestamp),
            "ingested_at": ingested_at_iso,
            "published_at": published_at_iso,
            "route_id": trip.route_id or None,
            "trip_id": trip.trip_id or None,
            "vehicle_id": vehicle_id,
            "payload": {
                "latitude": position.latitude,
                "longitude": position.longitude,
                "bearing": (
                    position.bearing
                    if position.HasField("bearing")
                    else None),
                "odometer": (
                    position.odometer
                    if position.HasField("odometer")
                    else None
                ),
                "speed_mps": (
                    position.speed
                    if position.HasField("speed")
                    else None
                ),
                "direction_id": (
                    trip.direction_id
                    if trip.HasField("direction_id")
                    else None
                ),
                "current_stop_sequence": (
                    vehicle.current_stop_sequence
                    if vehicle.HasField("current_stop_sequence")
                    else None
                ),
                "stop_id": vehicle.stop_id or None,
                "current_status": (
                    gtfs_realtime_pb2.VehiclePosition.VehicleStopStatus.Name(
                        vehicle.current_status
                    )
                    if vehicle.HasField("current_status")
                    else None
                ),
                "vehicle_label": description.label or None,
                "occupancy_status": (
                    gtfs_realtime_pb2.VehiclePosition.OccupancyStatus.Name(
                        vehicle.occupancy_status
                    )
                     if vehicle.HasField("occupancy_status")
                    else None
                )
            }
        }

        events.append(event)

    return events
    

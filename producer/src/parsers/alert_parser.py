from datetime import UTC, datetime
from typing import Any

from google.transit import gtfs_realtime_pb2


def _to_iso(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp, UTC).isoformat()


def _translated_text(value: gtfs_realtime_pb2.TranslatedString) -> str | None:
    for translation in value.translation:
        if translation.language == "en":
            return translation.text

    if value.translation:
        return value.translation[0].text

    return None


def parse_service_alerts(
    feed: gtfs_realtime_pb2.FeedMessage, ingested_at: datetime
) -> list[dict[str, Any]]:

    if not feed.header.HasField("timestamp"):
        raise ValueError("Feed header is missing timestamp field")

    feed_timestamp = feed.header.timestamp
    ingested_at_iso = ingested_at.astimezone(UTC).isoformat()
    published_at_iso = datetime.now(UTC).isoformat()

    events: list[dict[str, Any]] = []

    for entity in feed.entity:
        if entity.is_deleted or not entity.HasField("alert"):
            continue

        alert = entity.alert

        if not alert.informed_entity:
            continue

        active_periods = []
        informed_entities = []

        for period in alert.active_period:
            active_periods.append(
                {
                    "start": (_to_iso(period.start) if period.HasField("start") else None),
                    "end": (_to_iso(period.end) if period.HasField("end") else None),
                }
            )

        for target in alert.informed_entity:
            trip = target.trip
            direction_id = None

            if target.HasField("direction_id"):
                direction_id = target.direction_id
            elif trip.HasField("direction_id"):
                direction_id = trip.direction_id

            informed_entities.append(
                {
                    "agency_id": target.agency_id or None,
                    "route_id": (target.route_id or trip.route_id or None),
                    "route_type": (target.route_type if target.HasField("route_type") else None),
                    "trip_id": trip.trip_id or None,
                    "stop_id": target.stop_id or None,
                    "direction_id": direction_id,
                }
            )

        event = {
            "event_id": f"{entity.id}:{feed_timestamp}",
            "event_type": "service_alert",
            "schema_version": 1,
            "source": "mbta_gtfs_realtime",
            "source_timestamp": _to_iso(feed_timestamp),
            "feed_timestamp": _to_iso(feed_timestamp),
            "ingested_at": ingested_at_iso,
            "published_at": published_at_iso,
            "alert_id": entity.id,
            "payload": {
                "cause": (
                    gtfs_realtime_pb2.Alert.Cause.Name(alert.cause)
                    if alert.HasField("cause")
                    else None
                ),
                "effect": (
                    gtfs_realtime_pb2.Alert.Effect.Name(alert.effect)
                    if alert.HasField("effect")
                    else None
                ),
                "severity": (
                    gtfs_realtime_pb2.Alert.SeverityLevel.Name(alert.severity_level)
                    if alert.HasField("severity_level")
                    else None
                ),
                "header_text": _translated_text(alert.header_text),
                "description_text": _translated_text(alert.description_text),
                "url": _translated_text(alert.url),
                "active_periods": active_periods,
                "informed_entities": informed_entities,
            },
        }

        events.append(event)

    return events

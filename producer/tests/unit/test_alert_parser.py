from datetime import UTC, datetime

from google.transit import gtfs_realtime_pb2

from producer.src.parsers.alert_parser import parse_service_alerts


def _add_alert(
    feed: gtfs_realtime_pb2.FeedMessage,
    alert_id: str,
    route_ids: list[str],
) -> None:
    entity = feed.entity.add()
    entity.id = alert_id
    entity.alert.cause = gtfs_realtime_pb2.Alert.MAINTENANCE
    entity.alert.effect = gtfs_realtime_pb2.Alert.DETOUR

    translation = entity.alert.header_text.translation.add()
    translation.text = f"Alert {alert_id}"
    translation.language = "en"

    for route_id in route_ids:
        entity.alert.informed_entity.add().route_id = route_id


def test_parse_service_alerts_creates_one_event_per_alert() -> None:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = 1_700_000_000

    _add_alert(feed, "alert-1", ["Red", "Orange"])
    _add_alert(feed, "alert-2", ["Green-B"])

    events = parse_service_alerts(
        feed,
        datetime(2026, 8, 11, 3, 0, tzinfo=UTC),
    )

    assert len(events) == 2
    assert events[0]["alert_id"] == "alert-1"
    assert events[0]["ingested_at"] == "2026-08-11T03:00:00+00:00"
    assert events[0]["published_at"].endswith("+00:00")
    assert [target["route_id"] for target in events[0]["payload"]["informed_entities"]] == [
        "Red",
        "Orange",
    ]
    assert events[1]["alert_id"] == "alert-2"


def test_parse_service_alerts_skips_alert_without_informed_entities() -> None:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = 1_700_000_000
    feed.entity.add().id = "empty-alert"
    feed.entity[0].alert.SetInParent()

    events = parse_service_alerts(feed, datetime.now(UTC))

    assert events == []

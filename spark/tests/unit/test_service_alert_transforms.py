import json
from datetime import datetime
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from spark.common.transforms.parsing import parse_service_alert_events
from spark.common.validation.data_quality import service_alert_quality, split_service_alert

FIXTURE_PATH = Path(__file__).resolve().parents[1] / "fixtures/service_alert_valid.json"


def _valid_event() -> dict[str, Any]:
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


def _raw_service_alert_df(
    spark_session: SparkSession,
    events: list[dict[str, Any]],
) -> DataFrame:
    rows = [
        (
            str(event["alert_id"]).encode(),
            json.dumps(event).encode(),
            "transit.service_alerts.v1",
            0,
            offset,
            datetime.fromisoformat("2026-08-21T00:00:00+00:00"),
        )
        for offset, event in enumerate(events)
    ]
    return spark_session.createDataFrame(
        rows,
        ["key", "value", "topic", "partition", "offset", "timestamp"],
    )


def test_parse_service_alert_preserves_nested_event_and_kafka_metadata(
    spark_session: SparkSession,
) -> None:
    parsed = parse_service_alert_events(
        _raw_service_alert_df(spark_session, [_valid_event()])
    ).first()

    assert parsed is not None
    assert parsed.alert_id == "alert-1"
    assert len(parsed.payload.informed_entities) == 2
    assert parsed.kafka_topic == "transit.service_alerts.v1"
    assert parsed.kafka_offset == 0


def test_service_alert_quality_splits_invalid_nested_values(
    spark_session: SparkSession,
) -> None:
    valid_event = _valid_event()
    valid_event["payload"]["active_periods"][0]["end"] = valid_event["payload"]["active_periods"][
        0
    ]["start"]

    invalid_direction = _valid_event()
    invalid_direction["event_id"] = "alert-invalid-direction"
    invalid_direction["payload"]["informed_entities"][0]["direction_id"] = 2

    invalid_route_type = _valid_event()
    invalid_route_type["event_id"] = "alert-invalid-route-type"
    invalid_route_type["payload"]["informed_entities"][0]["route_type"] = -1

    invalid_period = _valid_event()
    invalid_period["event_id"] = "alert-invalid-period"
    invalid_period["payload"]["active_periods"][0]["start"] = "2026-08-17T00:00:00+00:00"

    parsed_df = parse_service_alert_events(
        _raw_service_alert_df(
            spark_session,
            [valid_event, invalid_direction, invalid_route_type, invalid_period],
        )
    )
    valid_df, invalid_df = split_service_alert(service_alert_quality(parsed_df))

    valid_ids = {row.event_id for row in valid_df.select("event_id").collect()}
    invalid_ids = {row.event_id for row in invalid_df.select("event_id").collect()}

    assert valid_ids == {valid_event["event_id"]}
    assert invalid_ids == {
        invalid_direction["event_id"],
        invalid_route_type["event_id"],
        invalid_period["event_id"],
    }

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from spark.common.transforms.parsing import parse_trip_update_events
from spark.common.validation.data_quality import split_trip_update, trip_update_quality
from spark.jobs.route_reliability.transform import transform_trip_update

FIXTURE_PATH = Path(__file__).resolve().parents[1] / "fixtures/trip_update_valid.json"


def _valid_event() -> dict[str, Any]:
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


def _raw_trip_update_df(
    spark_session: SparkSession,
    events: list[dict[str, Any]],
) -> DataFrame:
    rows = [
        (
            str(event["trip_id"]).encode(),
            json.dumps(event).encode(),
            "transit.trip_updates.v1",
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


def test_parse_trip_update_preserves_nested_event_and_kafka_metadata(
    spark_session: SparkSession,
) -> None:
    parsed = parse_trip_update_events(_raw_trip_update_df(spark_session, [_valid_event()])).first()

    assert parsed is not None
    assert parsed.trip_id == "trip-1"
    assert len(parsed.payload.stop_time_updates) == 2
    assert parsed.kafka_topic == "transit.trip_updates.v1"
    assert parsed.kafka_offset == 0


def test_trip_update_quality_splits_valid_and_invalid_events(
    spark_session: SparkSession,
) -> None:
    valid_event = _valid_event()

    valid_empty_event = _valid_event()
    valid_empty_event["event_id"] = "trip-update-empty-stops"
    valid_empty_event["payload"]["direction_id"] = None
    valid_empty_event["payload"]["stop_time_updates"] = []

    invalid_event = _valid_event()
    invalid_event["event_id"] = "trip-update-invalid-direction"
    invalid_event["payload"]["direction_id"] = 2

    parsed_df = parse_trip_update_events(
        _raw_trip_update_df(
            spark_session,
            [valid_event, valid_empty_event, invalid_event],
        )
    )
    valid_df, invalid_df = split_trip_update(trip_update_quality(parsed_df))

    valid_ids = {row.event_id for row in valid_df.select("event_id").collect()}
    invalid_ids = {row.event_id for row in invalid_df.select("event_id").collect()}

    assert valid_ids == {valid_event["event_id"], valid_empty_event["event_id"]}
    assert invalid_ids == {invalid_event["event_id"]}


def test_transform_trip_update_explodes_and_flattens_stop_updates(
    spark_session: SparkSession,
) -> None:
    parsed_df = parse_trip_update_events(_raw_trip_update_df(spark_session, [_valid_event()]))
    result_df = transform_trip_update(parsed_df)
    rows = result_df.orderBy("stop_sequence").collect()

    assert [(row.stop_id, row.delay_seconds) for row in rows] == [
        ("stop-1", 120),
        ("stop-2", 180),
    ]
    assert rows[0].trip_id == "trip-1"
    assert rows[0].event_timestamp is not None
    assert "payload" not in result_df.columns

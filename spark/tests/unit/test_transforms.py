import json
from datetime import datetime
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from spark.common.transforms.parsing import parse_vehicle_position_events
from spark.common.validation.data_quality import (
    split_vehicle_position,
    vehicle_position_quality,
)
from spark.jobs.vehicle_state.transform import build_vehicle_state

FIXTURE_PATH = Path(__file__).resolve().parents[1] / "fixtures/vehicle_position_valid.json"


def _raw_vehicle_df(
    spark_session: SparkSession,
    events: list[dict[str, Any]],
) -> DataFrame:
    rows = [
        (
            str(event["vehicle_id"]).encode(),
            json.dumps(event).encode(),
            "transit.vehicle_positions.v1",
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


def _valid_event() -> dict[str, Any]:
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


def test_parse_vehicle_position_preserves_event_and_kafka_metadata(
    spark_session: SparkSession,
) -> None:
    parsed = parse_vehicle_position_events(_raw_vehicle_df(spark_session, [_valid_event()])).first()

    assert parsed is not None
    assert parsed.vehicle_id == "y1863"
    assert parsed.kafka_offset == 0
    assert parsed.payload.latitude == 42.33568572998047


def test_vehicle_quality_splits_valid_and_invalid_events(
    spark_session: SparkSession,
) -> None:
    valid_event = _valid_event()
    invalid_event = _valid_event()
    invalid_event["event_id"] = "invalid-position"
    invalid_event["payload"]["latitude"] = 999.0

    parsed_df = parse_vehicle_position_events(
        _raw_vehicle_df(spark_session, [valid_event, invalid_event])
    )
    valid_df, invalid_df = split_vehicle_position(vehicle_position_quality(parsed_df))

    assert [row.event_id for row in valid_df.select("event_id").collect()] == [
        valid_event["event_id"]
    ]
    assert [row.event_id for row in invalid_df.select("event_id").collect()] == ["invalid-position"]


def test_build_vehicle_state_flattens_payload(
    spark_session: SparkSession,
) -> None:
    parsed_df = parse_vehicle_position_events(_raw_vehicle_df(spark_session, [_valid_event()]))
    vehicle_state_df = build_vehicle_state(parsed_df)
    state = vehicle_state_df.first()

    assert state is not None
    assert state.vehicle_id == "y1863"
    assert state.latitude == 42.33568572998047
    assert state.event_timestamp is not None
    assert "payload" not in vehicle_state_df.columns

from pathlib import Path

from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType

from spark.common.schemas.service_alert import SERVICE_ALERT_SCHEMA
from spark.common.schemas.trip_update import TRIP_UPDATE_SCHEMA
from spark.common.schemas.vehicle_position import VEHICLE_POSITION_SCHEMA

FIXTURE_DIRECTORY = Path(__file__).resolve().parents[1] / "fixtures"


def _parse_fixture(
    spark_session: SparkSession,
    fixture_name: str,
    schema: StructType,
) -> Row | None:
    fixture_path = FIXTURE_DIRECTORY / fixture_name
    raw_json = fixture_path.read_text(encoding="utf-8")

    raw_df = spark_session.createDataFrame(
        [(raw_json,)],
        ["value"],
    )

    parsed_df = raw_df.select(
        from_json("value", schema).alias("event"),
    )

    return parsed_df.select("event.*").first()


def test_vehicle_position_schema_parses_valid_event(
    spark_session: SparkSession,
) -> None:
    event = _parse_fixture(
        spark_session,
        "vehicle_position_valid.json",
        VEHICLE_POSITION_SCHEMA,
    )

    assert event is not None
    assert event.vehicle_id is not None
    assert event.payload.latitude is not None
    assert event.payload.longitude is not None


def test_trip_update_schema_parses_valid_event(
    spark_session: SparkSession,
) -> None:
    event = _parse_fixture(
        spark_session,
        "trip_update_valid.json",
        TRIP_UPDATE_SCHEMA,
    )

    assert event is not None
    assert event.trip_id == "trip-1"
    assert event.payload.trip_delay_seconds == 90
    assert len(event.payload.stop_time_updates) == 2
    assert event.payload.stop_time_updates[0].stop_id == "stop-1"
    assert event.payload.stop_time_updates[0].predicted_arrival is not None


def test_service_alert_schema_parses_valid_event(
    spark_session: SparkSession,
) -> None:
    event = _parse_fixture(
        spark_session,
        "service_alert_valid.json",
        SERVICE_ALERT_SCHEMA,
    )

    assert event is not None
    assert event.alert_id == "alert-1"
    assert event.payload.cause == "MAINTENANCE"
    assert event.payload.effect == "DETOUR"
    assert len(event.payload.active_periods) == 1
    assert event.payload.active_periods[0].start is not None
    assert len(event.payload.informed_entities) == 2

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json

from spark.common.schemas.vehicle_position import VEHICLE_POSITION_SCHEMA

FIXTURE_PATH = Path(__file__).resolve().parents[1] / "fixtures" / "vehicle_position_valid.json"


def test_vehicle_position_schema_parses_valid_event(spark_session: SparkSession) -> None:
    raw_json = FIXTURE_PATH.read_text(encoding="utf-8")

    raw_df = spark_session.createDataFrame([(raw_json,)], ["value"])

    parsed_df = raw_df.select(from_json("value", VEHICLE_POSITION_SCHEMA).alias("event"))

    event = parsed_df.select("event.*").first()

    assert event is not None
    assert event.vehicle_id is not None
    assert event.payload.latitude is not None
    assert event.payload.longitude is not None

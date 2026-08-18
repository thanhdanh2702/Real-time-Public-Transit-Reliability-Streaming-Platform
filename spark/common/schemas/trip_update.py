from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

STOP_TIME_UPDATE_SCHEMA = StructType(
    [
        StructField("stop_id", StringType(), nullable=True),
        StructField("stop_sequence", IntegerType(), nullable=True),
        StructField("predicted_arrival", TimestampType(), nullable=True),
        StructField("predicted_departure", TimestampType(), nullable=True),
        StructField("delay_seconds", IntegerType(), nullable=True),
        StructField("schedule_relationship", StringType(), nullable=True),
    ]
)

PAYLOAD_SCHEMA = StructType(
    [
        StructField("direction_id", IntegerType(), nullable=False),
        StructField("schedule_relationship", StringType(), nullable=True),
        StructField("trip_delay_seconds", IntegerType(), nullable=True),
        StructField(
            "stop_time_updates",
            ArrayType(STOP_TIME_UPDATE_SCHEMA, containsNull=False),
            nullable=False,
        ),
    ]
)

TRIP_UPDATE_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("event_Type", StringType(), nullable=False),
        StructField("schema_version", IntegerType(), nullable=False),
        StructField("source", StringType(), nullable=False),
        StructField("source_timestamp", TimestampType(), nullable=False),
        StructField("feed_timestamp", TimestampType(), nullable=False),
        StructField("ingested_at", TimestampType(), nullable=False),
        StructField("published_at", TimestampType(), nullable=False),
        StructField("route_id", StringType(), nullable=True),
        StructField("trip_id", StringType(), nullable=False),
        StructField("vehicle_id", StringType(), nullable=True),
        StructField(
            "payload",
            PAYLOAD_SCHEMA,
            nullable=False,
        ),
    ]
)

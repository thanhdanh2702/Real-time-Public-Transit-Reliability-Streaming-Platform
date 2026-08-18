from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

VEHICLE_POSITION_PAYLOAD_SCHEMA = StructType(
    [
        StructField("latitude", DoubleType(), nullable=False),
        StructField("longitude", DoubleType(), nullable=False),
        StructField("bearing", DoubleType(), nullable=True),
        StructField("odometer", DoubleType(), nullable=True),
        StructField("speed_mps", DoubleType(), nullable=True),
        StructField("direction_id", IntegerType(), nullable=True),
        StructField("current_stop_sequence", IntegerType(), nullable=True),
        StructField("stop_id", StringType(), nullable=True),
        StructField("current_status", StringType(), nullable=True),
        StructField("vehicle_label", StringType(), nullable=True),
        StructField("occupancy_status", StringType(), nullable=True),
    ]
)

VEHICLE_POSITION_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("event_type", StringType(), nullable=False),
        StructField("schema_version", IntegerType(), nullable=False),
        StructField("source", StringType(), nullable=False),
        StructField("source_timestamp", TimestampType(), nullable=False),
        StructField("feed_timestamp", TimestampType(), nullable=False),
        StructField("ingested_at", TimestampType(), nullable=False),
        StructField("published_at", TimestampType(), nullable=False),
        StructField("route_id", StringType(), nullable=True),
        StructField("trip_id", StringType(), nullable=True),
        StructField("vehicle_id", StringType(), nullable=False),
        StructField(
            "payload",
            VEHICLE_POSITION_PAYLOAD_SCHEMA,
            nullable=False,
        ),
    ]
)

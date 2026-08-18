from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

SERVICE_ALERT_ACTIVE_PERIOD_SCHEMA = StructType(
    [
        StructField("start", TimestampType(), nullable=True),
        StructField("end", TimestampType(), nullable=True),
    ]
)


SERVICE_ALERT_INFORMED_ENTITY_SCHEMA = StructType(
    [
        StructField("agency_id", StringType(), nullable=True),
        StructField("route_id", StringType(), nullable=True),
        StructField("route_type", IntegerType(), nullable=True),
        StructField("trip_id", StringType(), nullable=True),
        StructField("stop_id", StringType(), nullable=True),
        StructField("direction_id", IntegerType(), nullable=True),
    ]
)


SERVICE_ALERT_PAYLOAD_SCHEMA = StructType(
    [
        StructField("cause", StringType(), nullable=True),
        StructField("effect", StringType(), nullable=True),
        StructField("severity", StringType(), nullable=True),
        StructField("header_text", StringType(), nullable=True),
        StructField("description_text", StringType(), nullable=True),
        StructField("tts_header_text", StringType(), nullable=True),
        StructField("tts_description_text", StringType(), nullable=True),
        StructField("url", StringType(), nullable=True),
        StructField(
            "active_periods",
            ArrayType(
                SERVICE_ALERT_ACTIVE_PERIOD_SCHEMA,
                containsNull=False,
            ),
            nullable=False,
        ),
        StructField(
            "informed_entities",
            ArrayType(
                SERVICE_ALERT_INFORMED_ENTITY_SCHEMA,
                containsNull=False,
            ),
            nullable=False,
        ),
    ]
)


SERVICE_ALERT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("event_type", StringType(), nullable=False),
        StructField("schema_version", IntegerType(), nullable=False),
        StructField("source", StringType(), nullable=False),
        StructField("source_timestamp", TimestampType(), nullable=False),
        StructField("feed_timestamp", TimestampType(), nullable=False),
        StructField("ingested_at", TimestampType(), nullable=False),
        StructField("published_at", TimestampType(), nullable=False),
        StructField("alert_id", StringType(), nullable=False),
        StructField(
            "payload",
            SERVICE_ALERT_PAYLOAD_SCHEMA,
            nullable=False,
        ),
    ]
)

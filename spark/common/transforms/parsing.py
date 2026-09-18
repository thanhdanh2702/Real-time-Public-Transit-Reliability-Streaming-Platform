from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType, StructField, StructType

from spark.common.schemas.service_alert import SERVICE_ALERT_SCHEMA
from spark.common.schemas.trip_update import TRIP_UPDATE_SCHEMA
from spark.common.schemas.vehicle_position import VEHICLE_POSITION_SCHEMA


def parse_vehicle_position_events(raw_df: DataFrame) -> DataFrame:

    kafka_df = raw_df.select(
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("raw_value"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
    )

    parsed_df = kafka_df.withColumn(
        "event",
        from_json(
            col("raw_value"),
            VEHICLE_POSITION_SCHEMA,
        ),
    )

    return parsed_df.select(
        "event.*",
        "kafka_key",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "raw_value",
    )


def parse_trip_update_events(raw_df: DataFrame) -> DataFrame:

    kafka_df = raw_df.select(
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("raw_value"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
    )

    parsed_df = kafka_df.withColumn(
        "event",
        from_json(
            col("raw_value"),
            TRIP_UPDATE_SCHEMA,
        ),
    )

    return parsed_df.select(
        "event.*",
        "kafka_key",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "raw_value",
    )


def parse_service_alert_events(raw_df: DataFrame) -> DataFrame:

    kafka_df = raw_df.select(
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("raw_value"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
    )

    # Preserve parse failures (e.g. malformed optional timestamps) for quality checks.
    # Extend a copy so the producer's event schema remains unchanged.
    parse_schema = StructType(
        [
            *SERVICE_ALERT_SCHEMA.fields,
            StructField("_corrupt_record", StringType(), True),
        ]
    )
    parsed_df = kafka_df.withColumn(
        "event",
        from_json(
            col("raw_value"),
            parse_schema,
            {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"},
        ),
    )

    return parsed_df.select(
        "event.*",
        "kafka_key",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "raw_value",
    )

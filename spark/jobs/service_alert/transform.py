from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def transform_service_alert(df: DataFrame) -> DataFrame:
    exploded_df = df.withColumn(
        "informed_entity",
        F.explode(F.col("payload.informed_entities")),
    )

    return exploded_df.select(
        F.col("event_id"),
        F.col("alert_id"),
        F.col("source_timestamp").alias("event_timestamp"),
        F.col("feed_timestamp"),
        F.col("ingested_at"),
        F.col("published_at"),
        F.col("payload.cause").alias("cause"),
        F.col("payload.effect").alias("effect"),
        F.col("payload.severity").alias("severity"),
        F.col("payload.header_text").alias("header_text"),
        F.col("payload.description_text").alias("description_text"),
        F.col("payload.url").alias("url"),
        F.col("payload.active_periods").alias("active_periods"),
        # Affected entity
        F.col("informed_entity.agency_id").alias("agency_id"),
        F.col("informed_entity.route_id").alias("route_id"),
        F.col("informed_entity.route_type").alias("route_type"),
        F.col("informed_entity.trip_id").alias("trip_id"),
        F.col("informed_entity.stop_id").alias("stop_id"),
        F.col("informed_entity.direction_id").alias("direction_id"),
        F.col("source"),
        F.col("schema_version"),
        F.col("kafka_topic"),
        F.col("kafka_partition"),
        F.col("kafka_offset"),
        F.col("kafka_timestamp"),
    )

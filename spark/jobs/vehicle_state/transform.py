from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_vehicle_state(df: DataFrame) -> DataFrame:
    return df.select(
        # event identity
        F.col("event_id"),
        F.col("vehicle_id"),
        # event time
        F.col("source_timestamp").alias("event_timestamp"),
        F.col("feed_timestamp"),
        F.col("published_at"),
        F.col("ingested_at"),
        # Transit identity
        F.col("route_id"),
        F.col("trip_id"),
        # vehicle state
        F.col("payload.latitude").alias("latitude"),
        F.col("payload.longitude").alias("longitude"),
        F.col("payload.bearing").alias("bearing"),
        F.col("payload.odometer").alias("odometer"),
        F.col("payload.speed_mps").alias("speed_mps"),
        F.col("payload.occupancy_status").alias("occupancy_status"),
        # data lineage
        F.col("source"),
        F.col("schema_version"),
        F.col("kafka_topic"),
        F.col("kafka_partition"),
        F.col("kafka_offset"),
        F.col("kafka_timestamp"),
    )

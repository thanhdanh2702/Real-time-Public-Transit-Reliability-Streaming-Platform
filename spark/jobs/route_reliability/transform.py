from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def transform_trip_update(df: DataFrame) -> DataFrame:

    explode_df = df.withColumn(
        "stop_update",
        F.explode(F.col("payload.stop_time_updates"))
    )

    return explode_df.select(
        F.col("event_id"),
        F.col("source_timestamp").alias("event_timestamp"),
        F.col("feed_timestamp"),
        F.col("ingested_at"),
        F.col("published_at"),

        F.col("trip_id"),
        F.col("route_id"),
        F.col("vehicle_id"),
        F.col("payload.direction_id").alias("direction_id"),
        F.col("payload.schedule_relationship").alias("trip_schedule_relationship"),
        F.col("payload.trip_delay_seconds"),

        F.col("stop_update.stop_id").alias("stop_id"),
        F.col("stop_update.stop_sequence").alias("stop_sequence"),
        F.col("stop_update.predicted_arrival").alias("predicted_arrival"),
        F.col("stop_update.predicted_departure").alias("predicted_departure"),
        F.col("stop_update.delay_seconds").alias("delay_seconds"),
        F.col("stop_update.schedule_relationship").alias("stop_schedule_relationship"),

        F.col("kafka_partition"),
        F.col("kafka_offset"),
        F.col("kafka_timestamp")
    )
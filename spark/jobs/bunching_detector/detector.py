from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

DEFAULT_HEADWAY_THRESHOLD_SECONDS = 120


def detect_bunching_candidates(
    df: DataFrame, headway_threshold_seconds: int = DEFAULT_HEADWAY_THRESHOLD_SECONDS
) -> DataFrame:
    trip_status = F.coalesce(F.col("trip_schedule_relationship"), F.lit("SCHEDULED"))

    stop_status = F.coalesce(F.col("stop_schedule_relationship"), F.lit("SCHEDULED"))

    eligible_df = (
        df.filter(F.col("feed_timestamp").isNotNull())
        .filter(F.col("route_id").isNotNull())
        .filter(F.col("direction_id").isNotNull())
        .filter(F.col("stop_id").isNotNull())
        .filter(F.col("trip_id").isNotNull())
        .filter(F.col("predicted_arrival").isNotNull())
        .filter(F.col("predicted_arrival") >= F.col("feed_timestamp"))
        .filter(~trip_status.isin("CANCELED", "DELETED"))
        .filter(~stop_status.isin("SKIPPED", "NO_DATA"))
    )

    arrival_window = Window.partitionBy(
        "feed_timestamp", "route_id", "direction_id", "stop_id"
    ).orderBy(F.col("predicted_arrival"), F.col("trip_id"))

    paired_df = (
        eligible_df.withColumn("leading_trip_id", F.lag("trip_id").over(arrival_window))
        .withColumn("leading_vehicle_id", F.lag("vehicle_id").over(arrival_window))
        .withColumn("leading_predicted_arrival", F.lag("predicted_arrival").over(arrival_window))
    )

    result_df = paired_df.withColumn(
        "headway_seconds",
        F.col("predicted_arrival").cast("long") - F.col("leading_predicted_arrival").cast("long"),
    )

    return result_df.filter(
        F.col("leading_trip_id").isNotNull()
        & (F.col("leading_trip_id") != F.col("trip_id"))
        & (F.col("headway_seconds") >= 0)
        & (F.col("headway_seconds") <= headway_threshold_seconds)
    ).select(
        "feed_timestamp",
        F.col("event_timestamp").alias("detected_at"),
        "route_id",
        "direction_id",
        "stop_id",
        "leading_trip_id",
        F.col("trip_id").alias("following_trip_id"),
        "leading_vehicle_id",
        F.col("vehicle_id").alias("following_vehicle_id"),
        "leading_predicted_arrival",
        F.col("predicted_arrival").alias("following_predicted_arrival"),
        "headway_seconds",
    )

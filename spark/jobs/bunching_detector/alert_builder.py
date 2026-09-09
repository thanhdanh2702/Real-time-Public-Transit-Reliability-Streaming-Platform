from pyspark.sql import DataFrame
from pyspark.sql import functions as F

RULE_VERSION = "bunching-v1"


def build_bunching_alerts(confirmed_df: DataFrame, threshold_seconds: int = 120) -> DataFrame:
    """Format confirmed observations as active operational alerts; no lifecycle state here."""
    identity = F.struct(
        F.lit("bus_bunching").alias("alert_type"),
        "route_id",
        "direction_id",
        "stop_id",
        "leading_trip_id",
        "following_trip_id",
    )
    alerts = confirmed_df.withColumn("alert_fingerprint", F.sha2(F.to_json(identity), 256))
    event_identity = F.struct(
        "alert_fingerprint",
        "first_detected_at",
        "last_detected_at",
        F.lit("active").alias("status"),
        F.lit(RULE_VERSION).alias("rule_version"),
    )
    vehicles = F.array_distinct(
        F.array_compact(F.array("leading_vehicle_id", "following_vehicle_id"))
    )
    evidence = F.struct(
        "stop_id",
        "leading_trip_id",
        "following_trip_id",
        "leading_predicted_arrival",
        "following_predicted_arrival",
    )
    return alerts.select(
        F.sha2(F.to_json(event_identity), 256).alias("event_id"),
        F.lit("operational_alert").alias("event_type"),
        F.lit(1).alias("schema_version"),
        F.lit("transitpulse_spark").alias("source"),
        F.col("last_detected_at").alias("source_timestamp"),
        "feed_timestamp",
        F.current_timestamp().alias("ingested_at"),
        F.current_timestamp().alias("published_at"),
        "route_id",
        F.col("following_trip_id").alias("trip_id"),
        F.col("following_vehicle_id").alias("vehicle_id"),
        F.struct(
            "alert_fingerprint",
            F.lit("bus_bunching").alias("alert_type"),
            F.lit("warning").alias("severity"),
            F.lit("active").alias("status"),
            "direction_id",
            vehicles.alias("related_vehicle_ids"),
            F.lit(RULE_VERSION).alias("rule_version"),
            F.lit(threshold_seconds).alias("threshold"),
            F.col("headway_seconds").alias("observed_value"),
            "first_detected_at",
            "last_detected_at",
            evidence.alias("evidence"),
        ).alias("payload"),
    )

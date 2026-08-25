from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

QUALITY_FLAG = "_is_valid"


def vehicle_position_condition() -> Column:
    condition = (
        F.col("event_id").isNotNull()
        & (F.length(F.trim(F.col("event_id"))) > 0)
        & (F.col("event_type") == "vehicle_position")
        & (F.col("schema_version") == 1)
        & (F.col("source") == "mbta_gtfs_realtime")
        & F.col("source_timestamp").isNotNull()
        & F.col("feed_timestamp").isNotNull()
        & F.col("ingested_at").isNotNull()
        & F.col("published_at").isNotNull()
        & F.col("vehicle_id").isNotNull()
        & (F.length(F.trim(F.col("vehicle_id"))) > 0)
        & F.col("payload").isNotNull()
        & F.col("payload.latitude").isNotNull()
        & F.col("payload.latitude").between(-90.0, 90.0)
        & F.col("payload.longitude").isNotNull()
        & F.col("payload.longitude").between(-180.0, 180.0)
    )

    return F.coalesce(condition, F.lit(False))


def trip_update_condition() -> Column:
    direction_id = F.col("payload.direction_id")

    condition = (
        F.col("event_id").isNotNull()
        & (F.length(F.trim(F.col("event_id"))) > 0)
        & (F.col("event_type") == "trip_update")
        & (F.col("schema_version") == 1)
        & (F.col("source") == "mbta_gtfs_realtime")
        & F.col("source_timestamp").isNotNull()
        & F.col("feed_timestamp").isNotNull()
        & F.col("ingested_at").isNotNull()
        & F.col("published_at").isNotNull()
        & F.col("trip_id").isNotNull()
        & (F.length(F.trim(F.col("trip_id"))) > 0)
        & F.col("payload").isNotNull()
        & F.col("payload.stop_time_updates").isNotNull()
        & (direction_id.isNull() | direction_id.isin(0, 1))
    )

    return F.coalesce(condition, F.lit(False))


def vehicle_position_quality(df: DataFrame) -> DataFrame:
    return df.withColumn(
        QUALITY_FLAG,
        vehicle_position_condition(),
    )


def trip_update_quality(df: DataFrame) -> DataFrame:
    return df.withColumn(
        QUALITY_FLAG,
        trip_update_condition(),
    )


def split_vehicle_position(checked_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    valid_df = checked_df.filter(F.col(QUALITY_FLAG)).drop(QUALITY_FLAG)
    invalid_df = checked_df.filter(~F.col(QUALITY_FLAG)).drop(QUALITY_FLAG)

    return valid_df, invalid_df


def split_trip_update(
    checked_df: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    valid_df = checked_df.filter(F.col(QUALITY_FLAG)).drop(QUALITY_FLAG)

    invalid_df = checked_df.filter(~F.col(QUALITY_FLAG)).drop(QUALITY_FLAG)

    return valid_df, invalid_df

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


def service_alert_condition() -> Column:
    informed_entities = F.col("payload.informed_entities")
    active_periods = F.col("payload.active_periods")

    valid_informed_entities = F.forall(
        informed_entities,
        lambda entity: (
            (entity["direction_id"].isNull() | entity["direction_id"].isin(0, 1))
            & (entity["route_type"].isNull() | (entity["route_type"] >= 0))
        ),
    )

    valid_active_periods = F.forall(
        active_periods,
        lambda period: (
            period["start"].isNull() | period["end"].isNull() | (period["start"] <= period["end"])
        ),
    )

    condition = (
        F.col("event_id").isNotNull()
        & (F.length(F.trim(F.col("event_id"))) > 0)
        & (F.col("event_type") == "service_alert")
        & (F.col("schema_version") == 1)
        & (F.col("source") == "mbta_gtfs_realtime")
        & F.col("source_timestamp").isNotNull()
        & F.col("feed_timestamp").isNotNull()
        & F.col("ingested_at").isNotNull()
        & F.col("published_at").isNotNull()
        & F.col("alert_id").isNotNull()
        & (F.length(F.trim(F.col("alert_id"))) > 0)
        & F.col("payload").isNotNull()
        & active_periods.isNotNull()
        & valid_active_periods
        & informed_entities.isNotNull()
        & (F.size(informed_entities) > 0)
        & valid_informed_entities
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


def service_alert_quality(df: DataFrame) -> DataFrame:
    return df.withColumn(
        QUALITY_FLAG,
        service_alert_condition(),
    )


def split_vehicle_position(checked_df: DataFrame) -> tuple[DataFrame, DataFrame]:

    valid_df = checked_df.filter(F.col(QUALITY_FLAG)).drop(QUALITY_FLAG)
    invalid_df = checked_df.filter(~F.col(QUALITY_FLAG)).drop(QUALITY_FLAG)

    return valid_df, invalid_df


def split_trip_update(checked_df: DataFrame) -> tuple[DataFrame, DataFrame]:

    valid_df = checked_df.filter(F.col(QUALITY_FLAG)).drop(QUALITY_FLAG)
    invalid_df = checked_df.filter(~F.col(QUALITY_FLAG)).drop(QUALITY_FLAG)

    return valid_df, invalid_df


def split_service_alert(checked_df: DataFrame) -> tuple[DataFrame, DataFrame]:

    valid_df = checked_df.filter(F.col(QUALITY_FLAG)).drop(QUALITY_FLAG)
    invalid_df = checked_df.filter(~F.col(QUALITY_FLAG)).drop(QUALITY_FLAG)

    return valid_df, invalid_df

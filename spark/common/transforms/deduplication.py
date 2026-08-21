from pyspark.sql import DataFrame


def deduplicate_events(df: DataFrame) -> DataFrame:
    return df.dropDuplicatesWithinWatermark(
        ["event_id"]
    )
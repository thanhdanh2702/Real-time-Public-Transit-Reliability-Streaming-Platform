from pyspark.sql import DataFrame

DEFAULT_EVENT_TIME_COLUMN = "source_timestamp"
DEFAULT_WATERMARK_DELAY = "2 minutes"


def add_event_time(
    df: DataFrame,
    event_time_column: str = DEFAULT_EVENT_TIME_COLUMN,
    watermark_delay: str = DEFAULT_WATERMARK_DELAY,
) -> DataFrame:

    return df.withWatermark(event_time_column, watermark_delay)

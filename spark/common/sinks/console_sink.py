from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery


def start_console_sink(
    df: DataFrame,
    checkpoint_location: str,
    query_name: str,
    trigger_interval: str = "10 seconds",
    num_rows: int = 20,
) -> StreamingQuery:
    return (
        df.writeStream
        .format("console")
        .outputMode("append")
        .queryName(query_name)
        .option("checkpointLocation", checkpoint_location)
        .option("truncate", "false")
        .option("numRows", num_rows)
        .trigger(processingTime=trigger_interval)
        .start()
    )
from pyspark.sql import DataFrame, SparkSession


def read_kafka_stream(
    spark: SparkSession,
    bootstrap_servers: str,
    topic: str,
    starting_offsets: str = "latest",
    max_offsets_per_trigger: int | None = None,
) -> DataFrame:
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
    )
    if max_offsets_per_trigger is not None:
        if max_offsets_per_trigger <= 0:
            raise ValueError("max_offsets_per_trigger must be positive")
        reader = reader.option("maxOffsetsPerTrigger", max_offsets_per_trigger)
    return reader.load()

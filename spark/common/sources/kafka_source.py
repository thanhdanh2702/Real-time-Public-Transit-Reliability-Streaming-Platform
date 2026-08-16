from pyspark.sql import DataFrame, SparkSession


def read_kafka_stream(
    spark: SparkSession,
    bootstrap_servers: str,
    topic: str,
    starting_offsets: str = "latest",
) -> DataFrame:
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .load()
    )

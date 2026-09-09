import os

from spark.common.session import create_spark_session
from spark.common.sinks.console_sink import start_console_sink
from spark.common.sources.kafka_source import read_kafka_stream
from spark.common.transforms import deduplication, event_time, parsing
from spark.common.validation import data_quality
from spark.jobs.route_reliability.transform import transform_trip_update

APP_NAME = "transitpulse-trip-update"
DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
DEFAULT_TRIP_UPDATES_TOPIC = "transit.trip_updates.v1"
DEFAULT_CHECKPOINT_LOCATION = "/opt/spark/checkpoints/trip-updates-console-v1"


def main() -> None:

    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_KAFKA_BOOTSTRAP_SERVERS)
    topic = os.getenv("KAFKA_TRIP_UPDATES_TOPIC", DEFAULT_TRIP_UPDATES_TOPIC)
    starting_offset = os.getenv("KAFKA_STARTING_OFFSETS", "latest")
    checkpoint = os.getenv("TRIP_UPDATE_CHECKPOINT_LOCATION", DEFAULT_CHECKPOINT_LOCATION)

    spark = create_spark_session(APP_NAME)

    try:
        raw_df = read_kafka_stream(
            spark=spark,
            bootstrap_servers=kafka_servers,
            topic=topic,
            starting_offsets=starting_offset,
        )

        parsed_df = parsing.parse_trip_update_events(raw_df)

        checked_df = data_quality.trip_update_quality(parsed_df)

        valid_df, _invalid_df = data_quality.split_trip_update(checked_df)

        event_df = event_time.add_event_time(valid_df)

        dedup_df = deduplication.deduplicate_events(event_df)

        res_df = transform_trip_update(dedup_df)

        query = start_console_sink(
            df=res_df,
            checkpoint_location=checkpoint,
            query_name=APP_NAME,
        )

        query.awaitTermination()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

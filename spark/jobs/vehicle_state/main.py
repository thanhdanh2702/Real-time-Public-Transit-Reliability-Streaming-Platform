import os

from spark.common.session import create_spark_session
from spark.common.sinks.console_sink import start_console_sink
from spark.common.sources.kafka_source import read_kafka_stream
from spark.common.transforms import deduplication, event_time, parsing
from spark.common.validation.data_quality import split_vehicle_position, vehicle_position_quality
from spark.jobs.vehicle_state.transform import build_vehicle_state

APP_NAME = "transitpulse-vehicle-state"
DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
DEFAULT_VEHICLE_TOPIC = "transit.vehicle_positions.v1"
DEFAULT_CHECKPOINT_LOCATION = "/opt/spark/checkpoints/vehicle-state-console-v1"


def main() -> None:
    bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        DEFAULT_KAFKA_BOOTSTRAP_SERVERS,
    )
    vehicle_topic = os.getenv(
        "KAFKA_VEHICLE_POSITIONS_TOPIC",
        DEFAULT_VEHICLE_TOPIC,
    )
    starting_offsets = os.getenv(
        "KAFKA_STARTING_OFFSETS",
        "latest",
    )
    checkpoint_location = os.getenv(
        "VEHICLE_STATE_CHECKPOINT_LOCATION",
        DEFAULT_CHECKPOINT_LOCATION,
    )

    spark = create_spark_session(APP_NAME)

    try:
        raw_df = read_kafka_stream(
            spark=spark,
            bootstrap_servers=bootstrap_servers,
            topic=vehicle_topic,
            starting_offsets=starting_offsets,
        )

        parsed_df = parsing.parse_vehicle_position_events(raw_df)

        checked_df = vehicle_position_quality(parsed_df)

        valid_df, _invalid_df = split_vehicle_position(checked_df)

        event_time_df = event_time.add_event_time(valid_df)

        deduplicated_df = deduplication.deduplicate_events(event_time_df)

        vehicle_state_df = build_vehicle_state(deduplicated_df)

        query = start_console_sink(
            df=vehicle_state_df,
            checkpoint_location=checkpoint_location,
            query_name=APP_NAME,
        )

        query.awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

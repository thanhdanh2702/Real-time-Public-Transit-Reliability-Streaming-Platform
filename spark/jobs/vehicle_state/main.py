import os

from spark.common.sources.kafka_source import read_kafka_stream
from spark.common.transforms.parsing import parse_vehicle_position_events
from spark.common.session import create_spark_session

APP_NAME = "transitpulse-vehicle-state"
DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
DEFAULT_VEHICLE_TOPIC = "transit.vehicle_positions.v1"
DEFAULT_CHECKPOINT_LOCATION = "/opt/spark/checkpoints/vehicle-state"

def main() -> None:

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_KAFKA_BOOTSTRAP_SERVERS)
    vehicle_topic = os.getenv("KAFKA_VEHICLE_POSITIONS_TOPIC", DEFAULT_VEHICLE_TOPIC)
    starting_offsets = os.getenv("KAFKA_STARTING_OFFSETS", "latest")
    checkpoint_location = os.getenv("VEHICLE_STATE_CHECKPOINT_LOCATION", DEFAULT_CHECKPOINT_LOCATION)

    spark = create_spark_session(APP_NAME)

    raw_df = read_kafka_stream(
        spark=spark,
        bootstrap_servers=bootstrap_servers,
        topic=vehicle_topic,
        starting_offsets=starting_offsets
    )

    parsed_df = parse_vehicle_position_events(raw_df)

    query = (
        parsed_df.writeStream
        .format("console")
        .option("checkpointLocation", checkpoint_location)
        .trigger(processingTime="10 seconds")
        .start()
    )

    try:
        query.awaitTermination()

    finally:
        spark.stop()

if __name__ == "__main__":
    main()


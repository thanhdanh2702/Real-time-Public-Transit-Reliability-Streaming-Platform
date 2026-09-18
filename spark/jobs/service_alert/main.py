import os

from spark.common.config import postgres_connection_from_env
from spark.common.session import create_spark_session
from spark.common.sinks.postgres_sink import start_postgres_sink
from spark.common.sources.kafka_source import read_kafka_stream
from spark.common.transforms import deduplication, event_time, parsing
from spark.common.validation import data_quality
from spark.jobs.service_alert.transform import transform_service_alert

APP_NAME = "transitpulse-service-alert"
DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
DEFAULT_SERVICE_ALERTS_TOPIC = "transit.service_alerts.v1"
DEFAULT_CHECKPOINT_LOCATION = "/opt/spark/checkpoints/service-alert-postgres-v1"

POSTGRES_SCHEMA = "staging"
POSTGRES_TABLE = "service_alert_entities"


def main() -> None:
    bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        DEFAULT_KAFKA_BOOTSTRAP_SERVERS,
    )
    topic = os.getenv(
        "KAFKA_SERVICE_ALERTS_TOPIC",
        DEFAULT_SERVICE_ALERTS_TOPIC,
    )
    starting_offsets = os.getenv("KAFKA_STARTING_OFFSETS", "latest")
    checkpoint_location = os.getenv(
        "SERVICE_ALERT_CHECKPOINT_LOCATION",
        DEFAULT_CHECKPOINT_LOCATION,
    )

    postgres_connection = postgres_connection_from_env()

    spark = create_spark_session(APP_NAME)

    try:
        raw_df = read_kafka_stream(
            spark=spark,
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            starting_offsets=starting_offsets,
        )

        parsed_df = parsing.parse_service_alert_events(raw_df)
        checked_df = data_quality.service_alert_quality(parsed_df)
        valid_df, _invalid_df = data_quality.split_service_alert(checked_df)
        event_time_df = event_time.add_event_time(valid_df)
        deduplicated_df = deduplication.deduplicate_events(event_time_df)
        service_alert_df = transform_service_alert(deduplicated_df)

        query = start_postgres_sink(
            df=service_alert_df,
            connection=postgres_connection,
            schema=POSTGRES_SCHEMA,
            table=POSTGRES_TABLE,
            checkpoint_location=checkpoint_location,
            query_name=APP_NAME,
            json_columns=("active_periods",),
        )
        query.awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

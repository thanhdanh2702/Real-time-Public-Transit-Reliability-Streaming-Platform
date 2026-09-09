import os
from pathlib import Path

import yaml

from spark.common.session import create_spark_session
from spark.common.sources.kafka_source import read_kafka_stream
from spark.jobs.bunching_detector.pipeline import BunchingSettings, start_bunching_query

APP_NAME = "transitpulse-trip-update-bunching"
DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
DEFAULT_TRIP_UPDATES_TOPIC = "transit.trip_updates.v1"
DEFAULT_CHECKPOINT_LOCATION = "/opt/spark/checkpoints/trip-update-bunching-v1"


def load_settings() -> BunchingSettings:
    default_path = Path(__file__).resolve().parents[3] / "configs/thresholds.yaml"
    path = Path(os.getenv("THRESHOLDS_CONFIG_PATH", str(default_path)))
    config = yaml.safe_load(path.read_text())
    bunching = config["bunching"]
    routes_path = Path(
        os.getenv("ROUTES_CONFIG_PATH", str(default_path.with_name("routes.example.yaml")))
    )
    routes = yaml.safe_load(routes_path.read_text())
    selected_routes = tuple(
        route for route in routes["allowlist"] if route not in routes.get("exclude", [])
    )
    return BunchingSettings(
        route_ids=selected_routes,
        **{
            name: bunching[name]
            for name in BunchingSettings.__dataclass_fields__
            if name in bunching and name != "route_ids"
        },
    )


def main() -> None:

    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_KAFKA_BOOTSTRAP_SERVERS)
    topic = os.getenv("KAFKA_TRIP_UPDATES_TOPIC", DEFAULT_TRIP_UPDATES_TOPIC)
    starting_offset = os.getenv("KAFKA_STARTING_OFFSETS", "latest")
    checkpoint = os.getenv("TRIP_UPDATE_CHECKPOINT_LOCATION", DEFAULT_CHECKPOINT_LOCATION)

    settings = load_settings()
    if "://" in checkpoint:
        raise ValueError("This local state backend requires a filesystem checkpoint path")
    spark = create_spark_session(APP_NAME)

    try:
        raw_df = read_kafka_stream(
            spark=spark,
            bootstrap_servers=kafka_servers,
            topic=topic,
            starting_offsets=starting_offset,
            max_offsets_per_trigger=settings.max_events_per_batch,
        )

        query = start_bunching_query(
            spark,
            raw_df,
            Path(checkpoint),
            settings,
            trigger_interval=os.getenv("TRIP_UPDATE_TRIGGER_INTERVAL", "10 seconds"),
        )

        query.awaitTermination()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

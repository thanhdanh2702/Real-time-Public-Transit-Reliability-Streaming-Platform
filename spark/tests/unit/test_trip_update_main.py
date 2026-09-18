from unittest.mock import MagicMock

import pytest

import spark.jobs.route_reliability.main as trip_update_main


@pytest.mark.parametrize("failure_stage", [None, "sink", "await"])
def test_main_writes_trip_stop_updates_to_postgres(
    monkeypatch: pytest.MonkeyPatch,
    failure_stage: str | None,
) -> None:
    spark = MagicMock()
    query = MagicMock()
    raw_df = object()
    parsed_df = object()
    checked_df = object()
    valid_df = object()
    invalid_df = object()
    event_time_df = object()
    deduplicated_df = object()
    trip_stop_df = object()

    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker:19092")
    monkeypatch.setenv("KAFKA_TRIP_UPDATES_TOPIC", "trip-updates.test")
    monkeypatch.setenv("KAFKA_STARTING_OFFSETS", "earliest")
    monkeypatch.setenv("TRIP_UPDATE_CHECKPOINT_LOCATION", "/tmp/trip-update-checkpoint")
    monkeypatch.setenv("POSTGRES_HOST", "database.test")
    monkeypatch.setenv("POSTGRES_PORT", "15432")
    monkeypatch.setenv("POSTGRES_DB", "transit_test")
    monkeypatch.setenv("POSTGRES_USER", "transit_user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "test-password")

    create_session = MagicMock(return_value=spark)
    read_stream = MagicMock(return_value=raw_df)
    parse_events = MagicMock(return_value=parsed_df)
    check_quality = MagicMock(return_value=checked_df)
    split_events = MagicMock(return_value=(valid_df, invalid_df))
    add_event_time = MagicMock(return_value=event_time_df)
    deduplicate = MagicMock(return_value=deduplicated_df)
    transform = MagicMock(return_value=trip_stop_df)
    start_sink = MagicMock(return_value=query)
    if failure_stage == "sink":
        start_sink.side_effect = RuntimeError("sink failed")
    elif failure_stage == "await":
        query.awaitTermination.side_effect = RuntimeError("await failed")

    monkeypatch.setattr(trip_update_main, "create_spark_session", create_session)
    monkeypatch.setattr(trip_update_main, "read_kafka_stream", read_stream)
    monkeypatch.setattr(trip_update_main.parsing, "parse_trip_update_events", parse_events)
    monkeypatch.setattr(trip_update_main.data_quality, "trip_update_quality", check_quality)
    monkeypatch.setattr(trip_update_main.data_quality, "split_trip_update", split_events)
    monkeypatch.setattr(trip_update_main.event_time, "add_event_time", add_event_time)
    monkeypatch.setattr(
        trip_update_main.deduplication,
        "deduplicate_events",
        deduplicate,
    )
    monkeypatch.setattr(trip_update_main, "transform_trip_update", transform)
    monkeypatch.setattr(trip_update_main, "start_postgres_sink", start_sink)

    if failure_stage is None:
        trip_update_main.main()
    else:
        with pytest.raises(RuntimeError, match=f"{failure_stage} failed"):
            trip_update_main.main()

    create_session.assert_called_once_with("transitpulse-trip-update")
    read_stream.assert_called_once_with(
        spark=spark,
        bootstrap_servers="broker:19092",
        topic="trip-updates.test",
        starting_offsets="earliest",
    )
    parse_events.assert_called_once_with(raw_df)
    check_quality.assert_called_once_with(parsed_df)
    split_events.assert_called_once_with(checked_df)
    add_event_time.assert_called_once_with(valid_df)
    deduplicate.assert_called_once_with(event_time_df)
    transform.assert_called_once_with(deduplicated_df)
    start_sink.assert_called_once_with(
        df=trip_stop_df,
        connection={
            "host": "database.test",
            "port": 15432,
            "dbname": "transit_test",
            "user": "transit_user",
            "password": "test-password",
        },
        schema="staging",
        table="trip_stop_updates",
        checkpoint_location="/tmp/trip-update-checkpoint",
        query_name="transitpulse-trip-update",
    )
    if failure_stage == "sink":
        query.awaitTermination.assert_not_called()
    else:
        query.awaitTermination.assert_called_once_with()
    spark.stop.assert_called_once_with()


def test_main_stops_spark_when_stream_setup_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("POSTGRES_PASSWORD", "test-password")
    monkeypatch.setenv("POSTGRES_PORT", "5432")
    spark = MagicMock()
    monkeypatch.setattr(
        trip_update_main,
        "create_spark_session",
        MagicMock(return_value=spark),
    )
    monkeypatch.setattr(
        trip_update_main,
        "read_kafka_stream",
        MagicMock(side_effect=RuntimeError("Kafka unavailable")),
    )

    with pytest.raises(RuntimeError, match="Kafka unavailable"):
        trip_update_main.main()

    spark.stop.assert_called_once_with()

from unittest.mock import MagicMock

import pytest

import spark.jobs.service_alert.main as service_alert_main


def test_main_connects_service_alert_stream(monkeypatch: pytest.MonkeyPatch) -> None:
    spark = MagicMock()
    query = MagicMock()
    raw_df = object()
    parsed_df = object()
    checked_df = object()
    valid_df = object()
    invalid_df = object()
    event_time_df = object()
    deduplicated_df = object()
    alert_df = object()

    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker:19092")
    monkeypatch.setenv("KAFKA_SERVICE_ALERTS_TOPIC", "alerts.test")
    monkeypatch.setenv("KAFKA_STARTING_OFFSETS", "earliest")
    monkeypatch.setenv("SERVICE_ALERT_CHECKPOINT_LOCATION", "/tmp/alerts-checkpoint")

    create_session = MagicMock(return_value=spark)
    read_stream = MagicMock(return_value=raw_df)
    parse_events = MagicMock(return_value=parsed_df)
    check_quality = MagicMock(return_value=checked_df)
    split_events = MagicMock(return_value=(valid_df, invalid_df))
    add_event_time = MagicMock(return_value=event_time_df)
    deduplicate = MagicMock(return_value=deduplicated_df)
    transform = MagicMock(return_value=alert_df)
    start_sink = MagicMock(return_value=query)

    monkeypatch.setattr(service_alert_main, "create_spark_session", create_session)
    monkeypatch.setattr(service_alert_main, "read_kafka_stream", read_stream)
    monkeypatch.setattr(service_alert_main.parsing, "parse_service_alert_events", parse_events)
    monkeypatch.setattr(service_alert_main.data_quality, "service_alert_quality", check_quality)
    monkeypatch.setattr(service_alert_main.data_quality, "split_service_alert", split_events)
    monkeypatch.setattr(service_alert_main.event_time, "add_event_time", add_event_time)
    monkeypatch.setattr(
        service_alert_main.deduplication,
        "deduplicate_events",
        deduplicate,
    )
    monkeypatch.setattr(service_alert_main, "transform_service_alert", transform)
    monkeypatch.setattr(service_alert_main, "start_console_sink", start_sink)

    service_alert_main.main()

    create_session.assert_called_once_with("transitpulse-service-alert")
    read_stream.assert_called_once_with(
        spark=spark,
        bootstrap_servers="broker:19092",
        topic="alerts.test",
        starting_offsets="earliest",
    )
    parse_events.assert_called_once_with(raw_df)
    check_quality.assert_called_once_with(parsed_df)
    split_events.assert_called_once_with(checked_df)
    add_event_time.assert_called_once_with(valid_df)
    deduplicate.assert_called_once_with(event_time_df)
    transform.assert_called_once_with(deduplicated_df)
    start_sink.assert_called_once_with(
        df=alert_df,
        checkpoint_location="/tmp/alerts-checkpoint",
        query_name="transitpulse-service-alert",
    )
    query.awaitTermination.assert_called_once_with()
    spark.stop.assert_called_once_with()


def test_main_stops_spark_when_stream_setup_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spark = MagicMock()
    monkeypatch.setattr(
        service_alert_main,
        "create_spark_session",
        MagicMock(return_value=spark),
    )
    monkeypatch.setattr(
        service_alert_main,
        "read_kafka_stream",
        MagicMock(side_effect=RuntimeError("Kafka unavailable")),
    )

    with pytest.raises(RuntimeError, match="Kafka unavailable"):
        service_alert_main.main()

    spark.stop.assert_called_once_with()

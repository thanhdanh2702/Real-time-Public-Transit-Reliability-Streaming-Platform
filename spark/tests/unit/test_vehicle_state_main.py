from unittest.mock import MagicMock, sentinel

import pytest

from spark.jobs.vehicle_state import main as job


@pytest.mark.parametrize("failure_stage", [None, "source", "sink", "await"])
@pytest.mark.parametrize("checkpoint", [None, "/tmp/vehicle-test-checkpoint"])
def test_vehicle_main_wires_flow_and_always_stops_spark(monkeypatch, failure_stage, checkpoint):
    for name in (
        "POSTGRES_HOST",
        "POSTGRES_PORT",
        "POSTGRES_DB",
        "POSTGRES_USER",
        "VEHICLE_STATE_CHECKPOINT_LOCATION",
        "KAFKA_BOOTSTRAP_SERVERS",
        "KAFKA_STARTING_OFFSETS",
        "KAFKA_VEHICLE_POSITIONS_TOPIC",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("POSTGRES_PASSWORD", "test-password")
    if checkpoint:
        monkeypatch.setenv("VEHICLE_STATE_CHECKPOINT_LOCATION", checkpoint)

    spark, query = MagicMock(), MagicMock()
    create_session = MagicMock(return_value=spark)
    source = MagicMock(return_value=sentinel.raw)
    parse = MagicMock(return_value=sentinel.parsed)
    quality = MagicMock(return_value=sentinel.checked)
    split = MagicMock(return_value=(sentinel.valid, sentinel.invalid))
    event_time = MagicMock(return_value=sentinel.timed)
    dedup = MagicMock(return_value=sentinel.deduped)
    transform = MagicMock(return_value=sentinel.final)
    sink = MagicMock(return_value=query)
    if failure_stage:
        {"source": source, "sink": sink, "await": query.awaitTermination}[
            failure_stage
        ].side_effect = RuntimeError("test failure")

    monkeypatch.setattr(job, "create_spark_session", create_session)
    monkeypatch.setattr(job, "read_kafka_stream", source)
    monkeypatch.setattr(job.parsing, "parse_vehicle_position_events", parse)
    monkeypatch.setattr(job, "vehicle_position_quality", quality)
    monkeypatch.setattr(job, "split_vehicle_position", split)
    monkeypatch.setattr(job.event_time, "add_event_time", event_time)
    monkeypatch.setattr(job.deduplication, "deduplicate_events", dedup)
    monkeypatch.setattr(job, "build_vehicle_state", transform)
    monkeypatch.setattr(job, "start_postgres_sink", sink)

    if failure_stage:
        with pytest.raises(RuntimeError, match="test failure"):
            job.main()
    else:
        job.main()

    create_session.assert_called_once_with(job.APP_NAME)
    spark.stop.assert_called_once_with()
    source.assert_called_once_with(
        spark=spark,
        bootstrap_servers="kafka:9092",
        topic="transit.vehicle_positions.v1",
        starting_offsets="latest",
    )
    if failure_stage == "source":
        sink.assert_not_called()
        return

    parse.assert_called_once_with(sentinel.raw)
    quality.assert_called_once_with(sentinel.parsed)
    split.assert_called_once_with(sentinel.checked)
    event_time.assert_called_once_with(sentinel.valid)
    dedup.assert_called_once_with(sentinel.timed)
    transform.assert_called_once_with(sentinel.deduped)
    sink.assert_called_once_with(
        df=sentinel.final,
        connection={
            "host": "postgres",
            "port": 5432,
            "dbname": "transitpulse",
            "user": "transitpulse_admin",
            "password": "test-password",
        },
        schema="staging",
        table="vehicle_positions",
        query_name=job.APP_NAME,
        checkpoint_location=checkpoint or job.DEFAULT_CHECKPOINT_LOCATION,
    )
    if failure_stage == "sink":
        query.awaitTermination.assert_not_called()
    else:
        query.awaitTermination.assert_called_once_with()

"""Opt in with RUN_POSTGRES_INTEGRATION=1; only write into disposable schemas."""

import json
import os
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from pyspark.sql import functions as F

from spark.common.config import postgres_connection_from_env
from spark.common.sinks import postgres_sink
from spark.common.transforms import parsing
from spark.common.transforms.deduplication import deduplicate_events
from spark.common.transforms.event_time import add_event_time
from spark.common.validation import data_quality
from spark.jobs.route_reliability.transform import transform_trip_update
from spark.jobs.service_alert.transform import transform_service_alert
from spark.jobs.vehicle_state.transform import build_vehicle_state

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("RUN_POSTGRES_INTEGRATION") != "1",
        reason="Set RUN_POSTGRES_INTEGRATION=1 to test a real PostgreSQL database",
    ),
]
ROOT = Path(__file__).resolve().parents[3]
RAW_SCHEMA = (
    "key string, value string, topic string, partition int, offset long, timestamp timestamp"
)
FLOWS = {
    "vehicle_position": (
        parsing.parse_vehicle_position_events,
        data_quality.vehicle_position_quality,
        build_vehicle_state,
        "vehicle_positions",
        1,
    ),
    "trip_update": (
        parsing.parse_trip_update_events,
        data_quality.trip_update_quality,
        transform_trip_update,
        "trip_stop_updates",
        2,
    ),
    "service_alert": (
        parsing.parse_service_alert_events,
        data_quality.service_alert_quality,
        transform_service_alert,
        "service_alert_entities",
        2,
    ),
}


@pytest.fixture
def database():
    connection = postgres_connection_from_env()
    schema = f"test_spark_{uuid4().hex}"
    with psycopg.connect(**connection, autocommit=True) as db:
        db.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        try:
            for filename in (
                "002_create_vehicle_positions.sql",
                "003_create_trip_stop_updates.sql",
                "004_create_service_alert_entities.sql",
            ):
                ddl = (ROOT / "database/migrations" / filename).read_text()
                db.execute(ddl.replace("staging.", f"{schema}."))
            yield connection, schema, db
        finally:
            # Never target staging: schema is a fresh, test-owned UUID name.
            db.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


def _record(kind, offset):
    event = json.loads((ROOT / f"spark/tests/fixtures/{kind}_valid.json").read_text())
    event["event_id"] = f"test-{kind}-{offset}"
    return {
        "key": "test-key",
        "value": json.dumps(event),
        "topic": f"test.{kind}",
        "partition": 0,
        "offset": offset,
        "timestamp": "2026-08-21T00:00:00Z",
    }


def _output(raw, kind):
    parse, quality, transform, _, _ = FLOWS[kind]
    valid = (
        quality(parse(raw)).filter(F.col(data_quality.QUALITY_FLAG)).drop(data_quality.QUALITY_FLAG)
    )
    if raw.isStreaming:
        valid = deduplicate_events(add_event_time(valid))
    return transform(valid)


@pytest.mark.parametrize("kind", list(FLOWS))
@pytest.mark.parametrize("fail_after_commit", [False, True])
def test_stream_to_postgres_replay_and_checkpoint_restart(
    spark_session,
    tmp_path,
    database,
    monkeypatch,
    kind,
    fail_after_commit,
):
    connection, schema, db = database
    _, _, _, table, rows_per_event = FLOWS[kind]
    json_columns = ("active_periods",) if kind == "service_alert" else ()
    input_path = tmp_path / "input"
    input_path.mkdir()
    record = _record(kind, 0)
    (input_path / "0.json").write_text(json.dumps(record))
    raw = spark_session.readStream.schema(RAW_SCHEMA).json(str(input_path))
    output = _output(raw, kind)
    options = dict(connection=connection, schema=schema, table=table, json_columns=json_columns)
    real_write = postgres_sink.write_postgres_batch

    def fail_once_after_commit(batch_df, **kwargs):
        real_write(batch_df, **kwargs)
        raise RuntimeError("simulated crash after database commit")

    def start():
        return postgres_sink.start_postgres_sink(
            output,
            **options,
            checkpoint_location=str(tmp_path / "checkpoint"),
            query_name=f"test_{uuid4().hex}",
            trigger_interval="1 second",
        )

    def count():
        return db.execute(
            sql.SQL("SELECT count(*) FROM {}.{}").format(
                sql.Identifier(schema), sql.Identifier(table)
            )
        ).fetchone()[0]

    if fail_after_commit:
        monkeypatch.setattr(postgres_sink, "write_postgres_batch", fail_once_after_commit)
        query = start()
        try:
            with pytest.raises(Exception, match="simulated crash after database commit"):
                query.processAllAvailable()
            assert count() == rows_per_event
        finally:
            query.stop()
            monkeypatch.setattr(postgres_sink, "write_postgres_batch", real_write)

    query = start()
    try:
        query.processAllAvailable()
        (input_path / "1.json").write_text(json.dumps(record))
        query.processAllAvailable()
        assert count() == rows_per_event
    finally:
        query.stop()

    # A new event plus a duplicate after restart verifies checkpoint/state recovery.
    (input_path / "2.json").write_text(json.dumps(record) + "\n" + json.dumps(_record(kind, 1)))
    query = start()
    try:
        query.processAllAvailable()
        assert count() == 2 * rows_per_event
    finally:
        query.stop()

    # Bypass streaming dedup to exercise the database's idempotency guard directly.
    static_raw = spark_session.read.schema(RAW_SCHEMA).json(str(input_path / "0.json"))
    static_output = _output(static_raw, kind)
    real_write(static_output, **options)
    real_write(static_output.limit(0), **options)
    assert count() == 2 * rows_per_event
    if kind == "service_alert":
        periods = db.execute(
            sql.SQL("SELECT active_periods FROM {}.{}").format(
                sql.Identifier(schema), sql.Identifier(table)
            )
        ).fetchone()[0]
        assert isinstance(periods, list) and len(periods) == 1


def test_partition_transaction_rolls_back_on_constraint_error(spark_session, tmp_path, database):
    connection, schema, db = database
    records = [_record("vehicle_position", offset) for offset in range(2)]
    event = json.loads(records[1]["value"])
    event["payload"]["speed_mps"] = -1.0
    records[1]["value"] = json.dumps(event)
    path = tmp_path / "invalid.json"
    path.write_text("\n".join(map(json.dumps, records)))
    raw = spark_session.read.schema(RAW_SCHEMA).json(str(path))
    # Deliberately bypass quality to trigger an actual database constraint failure.
    output = build_vehicle_state(parsing.parse_vehicle_position_events(raw))
    options = dict(connection=connection, schema=schema, table="vehicle_positions", partitions=1)
    with pytest.raises(Exception, match="speed_check"):
        postgres_sink.write_postgres_batch(output, **options)

    statement = sql.SQL("SELECT count(*) FROM {}.vehicle_positions").format(sql.Identifier(schema))
    assert db.execute(statement).fetchone()[0] == 0
    postgres_sink.write_postgres_batch(output.withColumn("speed_mps", F.lit(0.0)), **options)
    assert db.execute(statement).fetchone()[0] == 2

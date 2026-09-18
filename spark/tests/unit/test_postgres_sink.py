from unittest.mock import MagicMock

import psycopg
import pytest
from pyspark.sql import Row

from spark.common.sinks import postgres_sink as sink


@pytest.mark.parametrize("empty", [False, True])
def test_batch_writes_all_rows_with_safe_identifiers_and_jsonb(monkeypatch, empty):
    rows = (
        []
        if empty
        else [Row(event_id="a", active_periods="[]"), Row(event_id="b", active_periods="[]")]
    )
    df = MagicMock()
    df.columns = ["event_id", "active_periods"]
    df.coalesce.return_value.foreachPartition.side_effect = lambda write: write(iter(rows))
    connect = MagicMock()
    monkeypatch.setattr(psycopg, "connect", connect)
    cursor = connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
    captured = []
    cursor.executemany.side_effect = lambda statement, values: captured.append(
        (statement.as_string(), list(values))
    )

    sink.write_postgres_batch(
        df,
        connection={"dbname": "test"},
        schema='schema"name',
        table="events",
        json_columns=("active_periods",),
        partitions=2,
    )

    df.coalesce.assert_called_once_with(2)
    if empty:
        connect.assert_not_called()
    else:
        connect.assert_called_once_with(dbname="test")
        statement, values = captured[0]
        assert '"schema""name"."events"' in statement
        assert '"event_id","active_periods"' in statement
        assert "%s,%s::jsonb" in statement
        assert "ON CONFLICT DO NOTHING" in statement
        assert values == [("a", "[]"), ("b", "[]")]


def test_database_error_is_propagated_for_spark_retry(monkeypatch):
    df = MagicMock()
    df.columns = ["event_id"]
    df.coalesce.return_value.foreachPartition.side_effect = lambda write: write([Row(event_id="a")])
    monkeypatch.setattr(
        psycopg, "connect", MagicMock(side_effect=psycopg.OperationalError("offline"))
    )

    with pytest.raises(psycopg.OperationalError, match="offline"):
        sink.write_postgres_batch(df, connection={}, schema="test", table="events")


def test_streaming_sink_configures_query_and_forwards_each_batch(monkeypatch):
    df, writer, batch = MagicMock(), MagicMock(), MagicMock()
    df.writeStream = writer
    for method in ("queryName", "option", "trigger", "foreachBatch"):
        getattr(writer, method).return_value = writer
    write_batch = MagicMock()
    monkeypatch.setattr(sink, "write_postgres_batch", write_batch)

    query = sink.start_postgres_sink(
        df,
        connection={"dbname": "test"},
        schema="staging",
        table="events",
        checkpoint_location="/tmp/checkpoint",
        query_name="test-sink",
        json_columns=("periods",),
        trigger_interval="5 seconds",
    )

    writer.queryName.assert_called_once_with("test-sink")
    writer.option.assert_called_once_with("checkpointLocation", "/tmp/checkpoint")
    writer.trigger.assert_called_once_with(processingTime="5 seconds")
    callback = writer.foreachBatch.call_args.args[0]
    callback(batch, 42)
    write_batch.assert_called_once_with(
        batch,
        connection={"dbname": "test"},
        schema="staging",
        table="events",
        json_columns=("periods",),
    )
    assert query is writer.start.return_value

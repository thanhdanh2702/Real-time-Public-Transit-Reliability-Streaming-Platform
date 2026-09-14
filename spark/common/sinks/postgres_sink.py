from collections.abc import Mapping
from itertools import chain
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery


def write_postgres_batch(
    batch_df: DataFrame,
    *,
    connection: Mapping[str, Any],
    schema: str,
    table: str,
    json_columns: tuple[str, ...] = (),
    partitions: int = 4,
) -> None:
    columns = tuple(batch_df.columns)
    json_columns_set = set(json_columns)

    def write_partition(rows) -> None:
        import psycopg
        from psycopg import sql

        rows = iter(rows)
        first_row = next(rows, None)

        if first_row is None:
            return

        placeholders = [
            sql.SQL("%s::jsonb") if column in json_columns_set else sql.Placeholder()
            for column in columns
        ]

        insert_statement = sql.SQL(
            """
            INSERT INTO {}.{} ({})
            VALUES ({})
            ON CONFLICT DO NOTHING
            """
        ).format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.SQL(",").join(map(sql.Identifier, columns)),
            sql.SQL(",").join(placeholders),
        )

        values = (tuple(row[column] for column in columns) for row in chain([first_row], rows))

        with (
            psycopg.connect(**connection) as postgres_connection,
            postgres_connection.cursor() as cursor,
        ):
            cursor.executemany(insert_statement, values)

    batch_df.coalesce(partitions).foreachPartition(write_partition)


def start_postgres_sink(
    df: DataFrame,
    *,
    connection: Mapping[str, Any],
    schema: str,
    table: str,
    checkpoint_location: str,
    query_name: str,
    json_columns: tuple[str, ...] = (),
    trigger_interval: str = "30 seconds",
) -> StreamingQuery:

    def write_batch(batch_df: DataFrame, _batch_id: int) -> None:
        write_postgres_batch(
            batch_df, connection=connection, schema=schema, table=table, json_columns=json_columns
        )

    return (
        df.writeStream.queryName(query_name)
        .option("checkpointLocation", checkpoint_location)
        .trigger(processingTime=trigger_interval)
        .foreachBatch(write_batch)
        .start()
    )

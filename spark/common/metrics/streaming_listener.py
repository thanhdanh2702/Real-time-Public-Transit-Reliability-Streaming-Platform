import json
import logging
from collections.abc import Mapping
from typing import Any

from pyspark.sql.streaming import StreamingQueryListener

LOGGER = logging.getLogger(__name__)


def _offsets_from_sources(sources: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    offsets: dict[str, dict[str, int]] = {}
    for source in sources:
        end_offset = source.get("endOffset", {})
        if isinstance(end_offset, str):
            try:
                end_offset = json.loads(end_offset)
            except json.JSONDecodeError:
                continue
        if not isinstance(end_offset, dict):
            continue
        for topic, partitions in end_offset.items():
            if not isinstance(partitions, dict):
                continue
            offsets[str(topic)] = {
                str(partition): int(value) for partition, value in partitions.items()
            }
    return offsets


def parse_progress(application_name: str, progress_json: str) -> dict[str, Any]:
    progress = json.loads(progress_json)
    durations = progress.get("durationMs") or {}
    return {
        "application_name": application_name,
        "query_id": str(progress["id"]),
        "run_id": str(progress["runId"]),
        "progress_timestamp": progress["timestamp"],
        "batch_id": int(progress["batchId"]),
        "num_input_rows": int(progress.get("numInputRows", 0)),
        "input_rows_per_second": float(progress.get("inputRowsPerSecond", 0.0)),
        "processed_rows_per_second": float(progress.get("processedRowsPerSecond", 0.0)),
        "batch_duration_ms": int(durations.get("triggerExecution", 0)),
        "source_offsets": _offsets_from_sources(progress.get("sources") or []),
    }


class PostgresStreamingMetricsListener(StreamingQueryListener):
    """Persist bounded driver-side Structured Streaming progress for operations UI."""

    def __init__(
        self,
        *,
        application_name: str,
        connection: Mapping[str, Any],
        retention_rows: int = 500,
    ) -> None:
        super().__init__()
        if retention_rows < 1:
            raise ValueError("retention_rows must be positive")
        self.application_name = application_name
        self.connection = dict(connection)
        self.retention_rows = retention_rows

    def onQueryStarted(self, event: Any) -> None:
        self._safe_status(
            query_id=str(event.id),
            run_id=str(event.runId),
            status="running",
            error_message=None,
        )

    def onQueryProgress(self, event: Any) -> None:
        try:
            self._write_progress(parse_progress(self.application_name, event.progress.json))
        except Exception as error:  # Metrics must never stop the streaming query.
            LOGGER.warning(
                "Could not persist Spark progress for %s: %s", self.application_name, error
            )

    def onQueryIdle(self, event: Any) -> None:
        self._safe_status(
            query_id=str(event.id),
            run_id=str(event.runId),
            status="idle",
            error_message=None,
        )

    def onQueryTerminated(self, event: Any) -> None:
        error_message = getattr(event, "exception", None)
        self._safe_status(
            query_id=str(event.id),
            run_id=str(event.runId),
            status="failed" if error_message else "terminated",
            error_message=str(error_message) if error_message else None,
        )

    def _safe_status(
        self,
        *,
        query_id: str,
        run_id: str,
        status: str,
        error_message: str | None,
    ) -> None:
        try:
            self._write_status(query_id, run_id, status, error_message)
        except Exception as error:  # Metrics must never stop the streaming query.
            LOGGER.warning(
                "Could not persist Spark status for %s: %s", self.application_name, error
            )

    def _write_status(
        self,
        query_id: str,
        run_id: str,
        status: str,
        error_message: str | None,
    ) -> None:
        import psycopg

        with psycopg.connect(**self.connection) as connection:
            connection.execute(
                """
                INSERT INTO ops.spark_application_status (
                    application_name, query_id, run_id, status, error_message, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (application_name) DO UPDATE SET
                    query_id = EXCLUDED.query_id,
                    run_id = EXCLUDED.run_id,
                    status = EXCLUDED.status,
                    error_message = EXCLUDED.error_message,
                    terminated_at = CASE
                        WHEN EXCLUDED.status IN ('terminated', 'failed') THEN CURRENT_TIMESTAMP
                    END,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (self.application_name, query_id, run_id, status, error_message),
            )

    def _write_progress(self, progress: dict[str, Any]) -> None:
        import psycopg
        from psycopg.types.json import Jsonb

        with psycopg.connect(**self.connection) as connection:
            connection.execute(
                """
                INSERT INTO ops.spark_streaming_progress (
                    application_name, query_id, run_id, progress_timestamp, batch_id,
                    num_input_rows, input_rows_per_second, processed_rows_per_second,
                    batch_duration_ms, source_offsets
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (query_id, run_id, batch_id) DO UPDATE SET
                    progress_timestamp = EXCLUDED.progress_timestamp,
                    num_input_rows = EXCLUDED.num_input_rows,
                    input_rows_per_second = EXCLUDED.input_rows_per_second,
                    processed_rows_per_second = EXCLUDED.processed_rows_per_second,
                    batch_duration_ms = EXCLUDED.batch_duration_ms,
                    source_offsets = EXCLUDED.source_offsets
                """,
                (
                    progress["application_name"],
                    progress["query_id"],
                    progress["run_id"],
                    progress["progress_timestamp"],
                    progress["batch_id"],
                    progress["num_input_rows"],
                    progress["input_rows_per_second"],
                    progress["processed_rows_per_second"],
                    progress["batch_duration_ms"],
                    Jsonb(progress["source_offsets"]),
                ),
            )
            connection.execute(
                """
                INSERT INTO ops.spark_application_status (
                    application_name, query_id, run_id, status, last_progress_at,
                    last_batch_id, updated_at
                )
                VALUES (%s, %s, %s, 'running', %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (application_name) DO UPDATE SET
                    query_id = EXCLUDED.query_id,
                    run_id = EXCLUDED.run_id,
                    status = 'running',
                    last_progress_at = EXCLUDED.last_progress_at,
                    last_batch_id = EXCLUDED.last_batch_id,
                    error_message = NULL,
                    terminated_at = NULL,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    progress["application_name"],
                    progress["query_id"],
                    progress["run_id"],
                    progress["progress_timestamp"],
                    progress["batch_id"],
                ),
            )
            connection.execute(
                """
                DELETE FROM ops.spark_streaming_progress
                WHERE progress_id IN (
                    SELECT progress_id
                    FROM ops.spark_streaming_progress
                    WHERE application_name = %s
                    ORDER BY progress_timestamp DESC, progress_id DESC
                    OFFSET %s
                )
                """,
                (self.application_name, self.retention_rows),
            )

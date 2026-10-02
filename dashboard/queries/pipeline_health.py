import json
import uuid
from datetime import datetime

import pandas as pd

from dashboard.queries.database import read_dataframe

STREAMING_TOPICS = (
    "transit.vehicle_positions.v1",
    "transit.trip_updates.v1",
    "transit.service_alerts.v1",
)
DLQ_TOPIC = "transit.dead_letter.v1"


def get_spark_health() -> pd.DataFrame:
    return read_dataframe(
        """
        WITH applications(application_name, display_name) AS (
            VALUES
                ('transitpulse-vehicle-state', 'Vehicle positions'),
                ('transitpulse-trip-update', 'Trip updates'),
                ('transitpulse-service-alert', 'Service alerts')
        )
        SELECT
            applications.application_name,
            applications.display_name,
            status.status,
            status.last_progress_at,
            progress.batch_id,
            progress.num_input_rows,
            progress.input_rows_per_second,
            progress.processed_rows_per_second,
            progress.batch_duration_ms,
            progress.source_offsets
        FROM applications
        LEFT JOIN ops.spark_application_status AS status
            USING (application_name)
        LEFT JOIN LATERAL (
            SELECT
                batch_id,
                num_input_rows,
                input_rows_per_second,
                processed_rows_per_second,
                batch_duration_ms,
                source_offsets
            FROM ops.spark_streaming_progress
            WHERE application_name = applications.application_name
            ORDER BY progress_timestamp DESC, progress_id DESC
            LIMIT 1
        ) AS progress ON TRUE
        ORDER BY applications.application_name
        """
    )


def get_latest_dbt_run() -> pd.DataFrame:
    return read_dataframe(
        """
        SELECT
            CASE
                WHEN status = 'running'
                    AND started_at < CURRENT_TIMESTAMP - INTERVAL '20 minutes'
                    THEN 'stale'
                ELSE status
            END AS status,
            started_at,
            finished_at,
            duration_seconds,
            CASE
                WHEN status = 'running'
                    AND started_at < CURRENT_TIMESTAMP - INTERVAL '20 minutes'
                    THEN COALESCE(error_message, 'Run did not reach a terminal state')
                ELSE error_message
            END AS error_message
        FROM ops.dbt_run_history
        ORDER BY started_at DESC
        LIMIT 1
        """
    )


def get_data_times() -> pd.DataFrame:
    return read_dataframe(
        """
        SELECT
            GREATEST(
                (SELECT MAX(event_timestamp) FROM staging.vehicle_positions),
                (SELECT MAX(event_timestamp) FROM staging.trip_stop_updates),
                (SELECT MAX(event_timestamp) FROM staging.service_alert_entities)
            ) AS latest_event_at,
            (
                SELECT MAX(finished_at)
                FROM ops.dbt_run_history
                WHERE status = 'success'
            ) AS latest_dbt_finished_at,
            CURRENT_TIMESTAMP AS dashboard_read_at
        """
    )


def _source_offsets(value: object) -> dict[str, dict[str, int]]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return {}
    if not isinstance(value, dict):
        return {}
    result: dict[str, dict[str, int]] = {}
    for topic, partitions in value.items():
        if not isinstance(partitions, dict):
            continue
        result[str(topic)] = {
            str(partition): int(offset) for partition, offset in partitions.items()
        }
    return result


def processed_offsets(progress: pd.DataFrame) -> dict[str, dict[int, int]]:
    offsets: dict[str, dict[int, int]] = {}
    if progress.empty or "source_offsets" not in progress:
        return offsets
    for value in progress["source_offsets"]:
        for topic, partitions in _source_offsets(value).items():
            offsets.setdefault(topic, {}).update(
                {int(partition): offset for partition, offset in partitions.items()}
            )
    return offsets


def get_kafka_watermarks(
    bootstrap_servers: str,
    topics: tuple[str, ...] = (*STREAMING_TOPICS, DLQ_TOPIC),
    *,
    timeout_seconds: float = 3.0,
) -> pd.DataFrame:
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"transitpulse-dashboard-{uuid.uuid4()}",
            "enable.auto.commit": False,
            "socket.timeout.ms": int(timeout_seconds * 1000),
        }
    )
    rows: list[dict[str, int | str]] = []
    try:
        metadata = consumer.list_topics(timeout=timeout_seconds)
        for topic in topics:
            topic_metadata = metadata.topics.get(topic)
            if topic_metadata is None or topic_metadata.error is not None:
                continue
            for partition in sorted(topic_metadata.partitions):
                low, high = consumer.get_watermark_offsets(
                    TopicPartition(topic, partition),
                    timeout=timeout_seconds,
                    cached=False,
                )
                rows.append(
                    {"topic": topic, "partition": partition, "low": low, "high": high}
                )
    finally:
        consumer.close()
    return pd.DataFrame(rows, columns=["topic", "partition", "low", "high"])


def calculate_kafka_backlog(
    watermarks: pd.DataFrame,
    offsets: dict[str, dict[int, int]],
) -> pd.DataFrame:
    result = watermarks.copy()
    if result.empty:
        result["processed_offset"] = pd.Series(dtype="Int64")
        result["backlog"] = pd.Series(dtype="Int64")
        return result

    def processed(row: pd.Series) -> int | None:
        return offsets.get(str(row["topic"]), {}).get(int(row["partition"]))

    result["processed_offset"] = result.apply(processed, axis=1)
    result["backlog"] = result.apply(
        lambda row: max(int(row["high"]) - int(row["processed_offset"]), 0)
        if pd.notna(row["processed_offset"])
        else None,
        axis=1,
    )
    return result


def retained_record_count(watermarks: pd.DataFrame, topic: str) -> int | None:
    topic_rows = watermarks[watermarks["topic"] == topic]
    if topic_rows.empty:
        return None
    return int((topic_rows["high"] - topic_rows["low"]).sum())


def display_status(
    status: object,
    last_progress_at: object,
    *,
    now: datetime | pd.Timestamp | None = None,
    stale_after_seconds: int = 120,
) -> str:
    if not isinstance(status, str) or not status:
        return "Unknown"
    normalized = status.lower()
    if normalized == "failed":
        return "Failed"
    if normalized == "terminated":
        return "Stopped"
    if pd.isna(last_progress_at):
        return "Unknown"
    current = pd.Timestamp.now(tz="UTC") if now is None else pd.to_datetime(now, utc=True)
    progress_time = pd.to_datetime(last_progress_at, utc=True)
    if current - progress_time > pd.Timedelta(seconds=stale_after_seconds):
        return "Stale"
    return "Idle" if normalized == "idle" else "Running"

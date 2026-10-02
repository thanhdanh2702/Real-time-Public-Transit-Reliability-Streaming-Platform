import os
import subprocess
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

DBT_MODELS = (
    "+vehicle_latest_state",
    "+route_health_5m",
    "+service_alerts_latest_snapshot",
    "+int_service_alert_periods",
)


def build_dbt_command() -> list[str]:
    return [
        "dbt",
        "build",
        "--project-dir",
        "/opt/airflow/dbt",
        "--profiles-dir",
        "/opt/airflow/include/dbt_profiles",
        "--select",
        *DBT_MODELS,
    ]


def _postgres_connection() -> dict[str, str | int]:
    return {
        "host": os.getenv("POSTGRES_HOST", "postgres"),
        "port": int(os.getenv("POSTGRES_HOST_PORT", "5432")),
        "dbname": os.getenv("POSTGRES_DB", "transitpulse"),
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
    }


def _record_started(connection: Mapping[str, Any], dag_run_id: str, started_at: datetime) -> str:
    import psycopg

    with psycopg.connect(**connection) as postgres_connection:
        row = postgres_connection.execute(
            """
            INSERT INTO ops.dbt_run_history (dag_run_id, status, started_at)
            VALUES (%s, 'running', %s)
            ON CONFLICT (dag_run_id) DO UPDATE SET
                status = 'running',
                started_at = EXCLUDED.started_at,
                finished_at = NULL,
                duration_seconds = NULL,
                error_message = NULL
            RETURNING run_id::TEXT
            """,
            (dag_run_id, started_at),
        ).fetchone()
    if row is None:
        raise RuntimeError("Could not create dbt run metadata")
    return str(row[0])


def _record_finished(
    connection: Mapping[str, Any],
    run_id: str,
    *,
    status: str,
    started_at: datetime,
    error_message: str | None,
) -> None:
    import psycopg

    finished_at = datetime.now(UTC)
    duration_seconds = (finished_at - started_at).total_seconds()
    with psycopg.connect(**connection) as postgres_connection:
        postgres_connection.execute(
            """
            UPDATE ops.dbt_run_history
            SET
                status = %s,
                finished_at = %s,
                duration_seconds = %s,
                error_message = %s
            WHERE run_id = %s::UUID
            """,
            (status, finished_at, duration_seconds, error_message, run_id),
        )


def run_dbt_refresh(dag_run_id: str, *, timeout_seconds: int = 480) -> None:
    connection = _postgres_connection()
    started_at = datetime.now(UTC)
    run_id = _record_started(connection, dag_run_id, started_at)
    try:
        subprocess.run(
            build_dbt_command(),
            check=True,
            timeout=timeout_seconds,
            env=os.environ.copy(),
        )
    except (subprocess.SubprocessError, OSError) as error:
        message = f"{type(error).__name__}: {error}"[:1000]
        _record_finished(
            connection,
            run_id,
            status="failed",
            started_at=started_at,
            error_message=message,
        )
        raise
    _record_finished(
        connection,
        run_id,
        status="success",
        started_at=started_at,
        error_message=None,
    )

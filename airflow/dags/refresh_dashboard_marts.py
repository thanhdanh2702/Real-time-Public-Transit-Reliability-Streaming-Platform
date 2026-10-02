import os
from datetime import UTC, datetime, timedelta

from airflow.sdk import dag, get_current_context, task
from transitpulse_airflow.dbt_refresh import run_dbt_refresh


@dag(
    dag_id="refresh_transitpulse_dashboard_marts",
    description="Build and test the dbt models consumed by the TransitPulse dashboard.",
    schedule=os.getenv("DBT_REFRESH_SCHEDULE", "*/5 * * * *"),
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=15),
    tags=["transitpulse", "dbt", "dashboard"],
)
def refresh_dashboard_marts():
    @task(
        retries=1,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=10),
    )
    def build_dashboard_marts() -> None:
        context = get_current_context()
        run_dbt_refresh(
            str(context["run_id"]),
            timeout_seconds=int(os.getenv("DBT_COMMAND_TIMEOUT_SECONDS", "480")),
        )

    build_dashboard_marts()


refresh_dashboard_marts()

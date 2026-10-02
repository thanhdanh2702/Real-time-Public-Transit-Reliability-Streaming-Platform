from pathlib import Path

from airflow.include.transitpulse_airflow.dbt_refresh import build_dbt_command

ROOT = Path(__file__).resolve().parents[2]
DAG_FILE = ROOT / "airflow/dags/refresh_dashboard_marts.py"
COMPOSE_FILE = ROOT / "docker-compose.yml"


def test_dbt_command_targets_dashboard_marts_and_dependencies():
    command = build_dbt_command()

    assert command[:5] == [
        "dbt",
        "build",
        "--project-dir",
        "/opt/airflow/dbt",
        "--profiles-dir",
    ]
    assert "+vehicle_latest_state" in command
    assert "+route_health_5m" in command
    assert "+service_alerts_latest_snapshot" in command
    assert "+int_service_alert_periods" in command
    assert isinstance(command, list)


def test_dag_uses_airflow_3_sdk_and_prevents_overlapping_runs():
    source = DAG_FILE.read_text()

    assert "from airflow.sdk import" in source
    assert "dag" in source and "task" in source
    assert "max_active_runs=1" in source
    assert "dagrun_timeout=" in source
    assert 'os.getenv("DBT_REFRESH_SCHEDULE"' in source
    assert "execution_timeout=" in source
    assert "retries=1" in source


def test_airflow_services_share_execution_api_configuration():
    source = COMPOSE_FILE.read_text()

    assert "AIRFLOW__CORE__EXECUTION_API_SERVER_URL:" in source
    assert "http://airflow-api-server:8080/execution/" in source
    assert (
        "AIRFLOW__API__SECRET_KEY: ${AIRFLOW_API_SECRET_KEY:?AIRFLOW_API_SECRET_KEY is required}"
        in source
    )
    assert (
        "AIRFLOW__API_AUTH__JWT_SECRET: "
        "${AIRFLOW_API_AUTH_JWT_SECRET:?AIRFLOW_API_AUTH_JWT_SECRET is required}"
        in source
    )
    assert "local-development-only-change-me" not in source

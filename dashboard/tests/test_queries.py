from datetime import UTC, datetime

import pytest
from sqlalchemy import create_engine

from dashboard.queries import database, overview


def test_database_engine_uses_environment_variables(monkeypatch):
    monkeypatch.setenv("POSTGRES_USER", "dashboard_user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "p@ss:/word")
    monkeypatch.setenv("POSTGRES_HOST", "postgres")
    monkeypatch.setenv("POSTGRES_PORT", "5432")
    monkeypatch.setenv("POSTGRES_DB", "transitpulse")

    engine = database.get_database_engine()

    assert engine is database.get_database_engine()
    assert engine.url.drivername == "postgresql+psycopg"
    assert engine.url.username == "dashboard_user"
    assert engine.url.password == "p@ss:/word"
    assert engine.url.host == "postgres"
    assert engine.url.port == 5432
    assert engine.url.database == "transitpulse"
    engine.dispose()
    database.get_database_engine.cache_clear()


def test_database_engine_requires_credentials(monkeypatch):
    monkeypatch.setenv("POSTGRES_USER", "dashboard_user")
    monkeypatch.delenv("POSTGRES_PASSWORD", raising=False)

    with pytest.raises(RuntimeError, match="POSTGRES_PASSWORD"):
        database.get_database_engine()


def test_read_dataframe_binds_parameters(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    monkeypatch.setattr(database, "get_database_engine", lambda: engine)

    with engine.begin() as connection:
        connection.exec_driver_sql("CREATE TABLE routes (route_id TEXT)")
        connection.exec_driver_sql("INSERT INTO routes (route_id) VALUES ('1')")

    result = database.read_dataframe(
        "SELECT route_id FROM routes WHERE route_id = :route_id",
        {"route_id": "1' OR 1=1 --"},
    )

    assert result.empty
    assert result.columns.tolist() == ["route_id"]
    engine.dispose()


def test_read_dataframe_returns_query_rows(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    monkeypatch.setattr(database, "get_database_engine", lambda: engine)

    result = database.read_dataframe("SELECT 1 AS vehicle_count")

    assert result.to_dict("records") == [{"vehicle_count": 1}]
    engine.dispose()


@pytest.fixture
def overview_database(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    monkeypatch.setattr(database, "get_database_engine", lambda: engine)
    with engine.begin() as connection:
        connection.exec_driver_sql("ATTACH DATABASE ':memory:' AS mart")
        connection.exec_driver_sql(
            "CREATE TABLE mart.vehicle_latest_state "
            "(event_timestamp TEXT, is_fresh BOOLEAN, route_type INTEGER)"
        )
        connection.exec_driver_sql(
            "CREATE TABLE mart.service_alerts_latest_snapshot "
            "(feed_timestamp TEXT, is_display_active_now BOOLEAN, is_feed_fresh BOOLEAN)"
        )
        connection.exec_driver_sql(
            "CREATE TABLE mart.route_health_5m "
            "(observation_bucket TEXT, observed_trip_count INTEGER, "
            "eligible_trip_count INTEGER, late_trip_count INTEGER)"
        )
    yield engine
    engine.dispose()


def test_overview_counts_only_fresh_vehicles_and_alerts(overview_database):
    with overview_database.begin() as connection:
        connection.exec_driver_sql(
            "INSERT INTO mart.vehicle_latest_state VALUES "
            "('2026-09-29 12:00:00', 1, 3), "
            "('2026-09-29 11:00:00', 0, 3), "
            "('2026-09-29 12:05:00', 1, 1)"
        )
        connection.exec_driver_sql(
            "INSERT INTO mart.service_alerts_latest_snapshot VALUES "
            "('2026-09-29 12:00:00', 1, 1), "
            "('2026-09-29 12:00:00', 1, 0), "
            "('2026-09-29 12:00:00', 0, 1)"
        )

    vehicle = overview.get_vehicle_status().iloc[0]
    alerts = overview.get_alert_status().iloc[0]

    assert vehicle["fresh_vehicle_count"] == 1
    assert vehicle["latest_event_at"] == "2026-09-29 12:00:00"
    assert alerts["active_alert_count"] == 1
    assert alerts["latest_feed_at"] == "2026-09-29 12:00:00"


def test_overview_health_trend_sums_counts_at_bucket_grain(overview_database):
    with overview_database.begin() as connection:
        connection.exec_driver_sql(
            "INSERT INTO mart.route_health_5m VALUES "
            "('2026-09-29 12:00:00', 1, 1, 1), "
            "('2026-09-29 12:00:00', 99, 99, 0), "
            "('2026-09-29 10:00:00', 10, 10, 10)"
        )

    trend = overview.get_health_trend(datetime(2026, 9, 29, 11, 0, tzinfo=UTC))
    latest = overview.get_latest_health_bucket().iloc[0]["latest_bucket"]

    assert len(trend) == 1
    assert trend.iloc[0]["observed_trip_count"] == 100
    assert trend.iloc[0]["eligible_trip_count"] == 100
    assert trend.iloc[0]["late_trip_count"] == 1
    assert trend.iloc[0]["predicted_late_percentage"] == 1.0
    assert latest == "2026-09-29 12:00:00"


def test_overview_empty_health_window_has_no_rows(overview_database):
    trend = overview.get_health_trend(datetime(2026, 9, 29, 11, 0, tzinfo=UTC))

    assert trend.empty
    assert overview.get_latest_health_bucket().iloc[0]["latest_bucket"] is None

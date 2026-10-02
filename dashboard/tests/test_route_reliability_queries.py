from datetime import UTC, datetime

import pandas as pd
import pytest
from sqlalchemy import create_engine

from dashboard.queries import database, route_reliability


@pytest.fixture
def mart_database(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    monkeypatch.setattr(database, "get_database_engine", lambda: engine)
    with engine.begin() as connection:
        connection.exec_driver_sql("ATTACH DATABASE ':memory:' AS mart")
        connection.exec_driver_sql(
            "CREATE TABLE mart.dim_routes "
            "(route_id TEXT, route_short_name TEXT, route_long_name TEXT, route_type INTEGER)"
        )
        connection.exec_driver_sql(
            "CREATE TABLE mart.route_health_5m "
            "(observation_bucket TEXT, route_id TEXT, route_short_name TEXT, "
            "direction_id INTEGER, observed_trip_count INTEGER, "
            "eligible_trip_count INTEGER, late_trip_count INTEGER)"
        )
        connection.exec_driver_sql(
            "CREATE TABLE mart.fct_trip_monitoring_samples "
            "(observation_bucket TEXT, observation_local_timestamp TEXT, "
            "event_timestamp TEXT, route_id TEXT, route_short_name TEXT, "
            "direction_id INTEGER, route_type INTEGER, trip_id TEXT, "
            "trip_instance_key TEXT, stop_name TEXT, delay_basis TEXT, "
            "scheduled_prediction_timestamp TEXT, delay_prediction_timestamp TEXT, "
            "predicted_delay_seconds INTEGER, is_predicted_late BOOLEAN)"
        )
        connection.exec_driver_sql(
            "INSERT INTO mart.dim_routes VALUES "
            "('A', 'A', 'Route A', 3), ('B', 'B', 'Route B', 3), ('Red', 'Red', 'Red Line', 1)"
        )
        connection.exec_driver_sql(
            "INSERT INTO mart.route_health_5m VALUES "
            "('2026-09-29 12:00:00', 'A', 'A', 0, 1, 1, 1), "
            "('2026-09-29 12:00:00', 'A', 'A', 1, 99, 99, 0), "
            "('2026-09-29 12:00:00', 'B', 'B', 0, 5, 0, 0), "
            "('2026-09-29 10:00:00', 'A', 'A', 0, 10, 10, 10)"
        )
        connection.exec_driver_sql(
            "INSERT INTO mart.fct_trip_monitoring_samples VALUES "
            "('2026-09-29 12:00:00', '2026-09-29 08:00:00', "
            "'2026-09-29 12:01:00', 'A', 'A', 0, 3, 'trip-1', 'trip-1|20260929', "
            "'Main St', 'arrival', '2026-09-29 12:05:00', '2026-09-29 12:11:00', 360, 1), "
            "('2026-09-29 12:00:00', '2026-09-29 08:00:00', "
            "'2026-09-29 12:02:00', 'A', 'A', 1, 3, 'trip-2', 'trip-2|20260929', "
            "'Park St', 'departure', '2026-09-29 12:05:00', '2026-09-29 12:05:00', 0, 0), "
            "('2026-09-29 12:00:00', '2026-09-29 08:00:00', "
            "'2026-09-29 12:03:00', 'Red', 'Red', 0, 1, 'trip-3', 'trip-3|20260929', "
            "'South St', 'arrival', '2026-09-29 12:05:00', '2026-09-29 12:05:00', 0, 0)"
        )
    yield engine
    engine.dispose()


def test_route_queries_use_bus_mart_and_weighted_rates(mart_database):
    since = datetime(2026, 9, 29, 11, 0, tzinfo=UTC)

    routes = route_reliability.get_routes()
    trend = route_reliability.get_trend(since)
    comparison = route_reliability.get_route_comparison(since).set_index("route_id")

    assert routes["route_id"].tolist() == ["A", "B"]
    assert trend.iloc[0]["observed_trip_count"] == 105
    assert trend.iloc[0]["eligible_trip_count"] == 100
    assert trend.iloc[0]["predicted_late_percentage"] == 1.0
    assert comparison.loc["A", "predicted_late_percentage"] == 1.0
    assert comparison.loc["B", "eligible_trip_count"] == 0


def test_route_comparison_applies_minimum_eligible_sample_threshold(mart_database):
    since = datetime(2026, 9, 29, 11, 0, tzinfo=UTC)

    comparison = route_reliability.get_route_comparison(
        since,
        min_eligible_samples=10,
    )

    assert comparison["route_id"].tolist() == ["A"]


def test_period_summary_uses_sample_grain_for_p90_and_equal_window(monkeypatch):
    calls = []

    def read(query, parameters=None):
        calls.append((query, parameters))
        return pd.DataFrame()

    monkeypatch.setattr(route_reliability, "read_dataframe", read)
    start = datetime(2026, 9, 29, 11, 0, tzinfo=UTC)
    end = datetime(2026, 9, 29, 12, 0, tzinfo=UTC)

    route_reliability.get_period_summary(start, end, route_id="A", direction_id=0)

    query, parameters = calls[0]
    assert "mart.fct_trip_monitoring_samples" in query
    assert "PERCENTILE_CONT(0.9)" in query
    assert "route_type = 3" in query
    assert "event_timestamp >= :start_time" in query
    assert "event_timestamp < :end_time" in query
    assert parameters == {
        "start_time": start,
        "end_time": end,
        "route_id": "A",
        "direction_id": 0,
    }


def test_route_filters_apply_to_trend_comparison_and_latest_bucket(mart_database):
    since = datetime(2026, 9, 29, 11, 0, tzinfo=UTC)

    trend = route_reliability.get_trend(since, route_id="A", direction_id=0)
    comparison = route_reliability.get_route_comparison(since, route_id="A", direction_id=0)
    latest = route_reliability.get_latest_bucket(route_id="A", direction_id=0).iloc[0]
    injected = route_reliability.get_trend(since, route_id="A' OR 1=1 --")

    assert trend.iloc[0]["observed_trip_count"] == 1
    assert trend.iloc[0]["predicted_late_percentage"] == 100.0
    assert comparison["route_id"].tolist() == ["A"]
    assert latest["latest_bucket"] == "2026-09-29 12:00:00"
    assert injected.empty


def test_trip_details_respect_filters_and_bus_scope(mart_database):
    since = datetime(2026, 9, 29, 11, 0, tzinfo=UTC)

    details = route_reliability.get_trip_samples(
        since,
        route_id="A",
        direction_id=0,
        bucket="2026-09-29 12:00:00",
        limit=1,
    )

    assert details["trip_id"].tolist() == ["trip-1"]
    assert details.iloc[0]["stop_name"] == "Main St"
    assert details.iloc[0]["delay_basis"] == "arrival"
    assert details.iloc[0]["predicted_delay_seconds"] == 360

    with pytest.raises(ValueError, match="limit"):
        route_reliability.get_trip_samples(since, limit=0)


def test_trip_details_support_safe_largest_delay_sort(monkeypatch):
    calls = []

    def read(query, parameters=None):
        calls.append((query, parameters))
        return pd.DataFrame()

    monkeypatch.setattr(route_reliability, "read_dataframe", read)
    since = datetime(2026, 9, 29, 11, 0, tzinfo=UTC)

    route_reliability.get_trip_samples(since, sort_by="largest_delay")

    query, _ = calls[0]
    assert "predicted_delay_seconds DESC" in query
    with pytest.raises(ValueError, match="sort_by"):
        route_reliability.get_trip_samples(since, sort_by="unsafe SQL")

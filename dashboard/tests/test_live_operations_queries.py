import pandas as pd
import pytest
from sqlalchemy import create_engine

from dashboard.queries import database, live_operations


@pytest.fixture
def live_database(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    monkeypatch.setattr(database, "get_database_engine", lambda: engine)
    with engine.begin() as connection:
        connection.exec_driver_sql("ATTACH DATABASE ':memory:' AS mart")
        connection.exec_driver_sql(
            "CREATE TABLE mart.vehicle_latest_state ("
            "vehicle_id TEXT, event_timestamp TEXT, route_id TEXT, route_type INTEGER, "
            "route_short_name TEXT, trip_id TEXT, trip_headsign TEXT, direction_id INTEGER, "
            "latitude REAL, longitude REAL, occupancy_status TEXT, is_fresh BOOLEAN)"
        )
        connection.exec_driver_sql(
            "CREATE TABLE mart.service_alerts_latest_snapshot ("
            "feed_timestamp TEXT, is_feed_fresh BOOLEAN)"
        )
    yield engine
    engine.dispose()


def test_live_vehicles_use_fresh_bus_positions_and_bound_route_filter(live_database):
    with live_database.begin() as connection:
        connection.exec_driver_sql(
            "INSERT INTO mart.vehicle_latest_state VALUES "
            "('bus-a', '2026-09-30 12:00:00', 'A', 3, 'A', 'trip-a', 'Center', 0, "
            "42.35, -71.06, 'MANY_SEATS_AVAILABLE', 1), "
            "('old-bus', '2026-09-30 11:00:00', 'A', 3, 'A', NULL, NULL, NULL, "
            "42.34, -71.05, NULL, 0), "
            "('train', '2026-09-30 12:01:00', 'A', 1, 'A', NULL, NULL, NULL, "
            "42.33, -71.04, NULL, 1), "
            "('no-location', '2026-09-30 12:02:00', 'A', 3, 'A', NULL, NULL, NULL, "
            "NULL, -71.03, NULL, 1), "
            "('bus-b', '2026-09-30 12:03:00', 'B', 3, 'B', NULL, NULL, NULL, "
            "42.36, -71.07, NULL, 1)"
        )

    vehicles = live_operations.get_live_vehicles(route_id="A", limit=10)
    malicious = live_operations.get_live_vehicles(route_id="A' OR 1=1 --", limit=10)
    status = live_operations.get_vehicle_feed_status().iloc[0]

    assert vehicles["vehicle_id"].tolist() == ["bus-a"]
    assert malicious.empty
    assert status["fresh_vehicle_count"] == 3
    assert status["latest_event_at"] == "2026-09-30 12:03:00"


def test_live_vehicles_fetch_one_extra_row_to_detect_truncation(live_database):
    with live_database.begin() as connection:
        connection.exec_driver_sql(
            "INSERT INTO mart.vehicle_latest_state VALUES "
            "('a', '2026-09-30 12:00:00', 'A', 3, 'A', NULL, NULL, NULL, "
            "42.35, -71.06, NULL, 1), "
            "('b', '2026-09-30 12:01:00', 'A', 3, 'A', NULL, NULL, NULL, "
            "42.36, -71.07, NULL, 1)"
        )

    vehicles = live_operations.get_live_vehicles(limit=1)

    assert vehicles["vehicle_id"].tolist() == ["b", "a"]


def test_alert_query_keeps_unknown_route_scope_and_uses_bound_values(monkeypatch):
    calls = []

    def read(query, parameters=None):
        calls.append((query, parameters))
        return pd.DataFrame()

    monkeypatch.setattr(live_operations, "read_dataframe", read)

    live_operations.get_active_alerts(route_id="A' OR 1=1 --", limit=20)

    query, parameters = calls[0]
    assert "is_display_active_now" in query
    assert "is_feed_fresh" in query
    assert "ANY(route_ids)" in query
    assert "CARDINALITY(route_ids) = 0" in query
    assert "A' OR 1=1 --" not in query
    assert parameters == {"route_id": "A' OR 1=1 --", "limit": 21}


def test_all_routes_omits_untyped_null_parameter(monkeypatch):
    calls = []

    def read(query, parameters=None):
        calls.append((query, parameters))
        return pd.DataFrame()

    monkeypatch.setattr(live_operations, "read_dataframe", read)

    live_operations.get_live_vehicles(limit=10)
    live_operations.get_active_alerts(limit=20)

    assert all(":route_id" not in query for query, _ in calls)
    assert calls[0][1] == {"limit": 11}
    assert calls[1][1] == {"limit": 21}


def test_alert_status_uses_latest_stored_snapshot(live_database):
    with live_database.begin() as connection:
        connection.exec_driver_sql(
            "INSERT INTO mart.service_alerts_latest_snapshot VALUES "
            "('2026-09-30 12:00:00', 1), ('2026-09-30 12:00:00', 1)"
        )

    status = live_operations.get_alert_feed_status().iloc[0]

    assert status["latest_feed_at"] == "2026-09-30 12:00:00"
    assert status["fresh_snapshot_entities"] == 2


@pytest.mark.parametrize("limit", [0, -1, 2001])
def test_live_queries_reject_invalid_limits(limit):
    with pytest.raises(ValueError, match="limit"):
        live_operations.get_live_vehicles(limit=limit)
    with pytest.raises(ValueError, match="limit"):
        live_operations.get_active_alerts(limit=limit)

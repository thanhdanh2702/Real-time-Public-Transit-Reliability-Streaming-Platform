from pathlib import Path

import pandas as pd
from streamlit.testing.v1 import AppTest

from dashboard.queries import live_operations, route_reliability

LIVE_PAGE = Path(__file__).resolve().parents[1] / "pages/3_live_operations.py"


def _mock_live_data(monkeypatch, *, vehicle_fresh=True, alert_fresh=True):
    now = pd.Timestamp.now(tz="UTC")
    old = now - pd.Timedelta(minutes=20)
    monkeypatch.setattr(
        route_reliability,
        "get_routes",
        lambda: pd.DataFrame(
            [{"route_id": "A", "route_short_name": "A", "route_long_name": "Route A"}]
        ),
    )
    monkeypatch.setattr(
        live_operations,
        "get_vehicle_feed_status",
        lambda: pd.DataFrame(
            [
                {
                    "latest_event_at": now if vehicle_fresh else old,
                    "fresh_vehicle_count": 1 if vehicle_fresh else 0,
                }
            ]
        ),
    )
    monkeypatch.setattr(
        live_operations,
        "get_alert_feed_status",
        lambda: pd.DataFrame(
            [
                {
                    "latest_feed_at": now if alert_fresh else old,
                    "fresh_snapshot_entities": 1 if alert_fresh else 0,
                }
            ]
        ),
    )


def test_live_page_shows_map_vehicle_detail_and_alert(monkeypatch):
    _mock_live_data(monkeypatch)
    calls = []

    def vehicles(route_id=None, limit=2000):
        calls.append(("vehicles", route_id, limit))
        return pd.DataFrame(
            [
                {
                    "vehicle_id": "bus-a",
                    "route_id": "A",
                    "route_short_name": "A",
                    "trip_id": "trip-a",
                    "trip_headsign": "Center",
                    "direction_id": 0,
                    "latitude": 42.35,
                    "longitude": -71.06,
                    "occupancy_status": "MANY_SEATS_AVAILABLE",
                    "event_timestamp": pd.Timestamp.now(tz="UTC"),
                }
            ]
        )

    def alerts(route_id=None, limit=100):
        calls.append(("alerts", route_id, limit))
        return pd.DataFrame(
            [
                {
                    "alert_id": "alert-a",
                    "severity": "WARNING",
                    "effect": "DETOUR",
                    "header_text": "Bus detour",
                    "description_text": "Route A is on detour.",
                    "route_ids": ["A"],
                }
            ]
        )

    monkeypatch.setattr(live_operations, "get_live_vehicles", vehicles)
    monkeypatch.setattr(live_operations, "get_active_alerts", alerts)

    page = AppTest.from_file(LIVE_PAGE).run()

    assert not page.exception
    assert [metric.value for metric in page.metric] == ["1", "1"]
    assert page.get("pydeck_chart")
    assert any("trip-a" in text.value for text in page.markdown)
    assert any("Bus detour" in text.value for text in page.markdown)
    next(widget for widget in page.selectbox if widget.label == "Route").set_value("A").run()
    assert calls[-2][1] == "A"
    assert calls[-1][1] == "A"


def test_live_page_does_not_show_old_positions_or_stale_alert_count(monkeypatch):
    _mock_live_data(monkeypatch, vehicle_fresh=False, alert_fresh=False)
    monkeypatch.setattr(live_operations, "get_live_vehicles", lambda **_kwargs: pd.DataFrame())
    monkeypatch.setattr(live_operations, "get_active_alerts", lambda **_kwargs: pd.DataFrame())

    page = AppTest.from_file(LIVE_PAGE).run()

    assert not page.exception
    assert [metric.value for metric in page.metric] == ["0", "—"]
    assert not page.get("pydeck_chart")
    assert len(page.warning) >= 2
    assert any("No fresh bus positions" in info.value for info in page.info)


def test_live_page_reports_database_failure_without_exposing_credentials(monkeypatch):
    def unavailable():
        raise RuntimeError("POSTGRES_PASSWORD=secret")

    monkeypatch.setattr(route_reliability, "get_routes", unavailable)

    page = AppTest.from_file(LIVE_PAGE).run()

    assert not page.exception
    assert any("PostgreSQL" in error.value for error in page.error)
    assert all("secret" not in error.value for error in page.error)

from pathlib import Path

import pandas as pd
from streamlit.testing.v1 import AppTest

from dashboard.queries import live_operations, overview, route_reliability

OVERVIEW_PAGE = Path(__file__).resolve().parents[1] / "pages/1_overview.py"
APP_PAGE = Path(__file__).resolve().parents[1] / "app.py"
THEME_FILE = Path(__file__).resolve().parents[2] / ".streamlit/config.toml"


def test_dashboard_entrypoint_opens_overview_by_default():
    app = AppTest.from_file(APP_PAGE).run()

    assert not app.exception
    assert app.title[0].value == "TransitPulse · Overview"


def test_dashboard_has_project_light_theme():
    theme = THEME_FILE.read_text()

    assert 'base = "light"' in theme
    assert 'primaryColor = "#2563EB"' in theme


def test_overview_page_shows_current_metrics(monkeypatch):
    now = pd.Timestamp.now(tz="UTC")
    bucket = now.floor("5min")
    monkeypatch.setattr(
        overview,
        "get_vehicle_status",
        lambda: pd.DataFrame([{"fresh_vehicle_count": 2, "latest_event_at": now}]),
    )
    monkeypatch.setattr(
        overview,
        "get_alert_status",
        lambda: pd.DataFrame([{"active_alert_count": 1, "latest_feed_at": now}]),
    )
    monkeypatch.setattr(
        overview,
        "get_latest_health_bucket",
        lambda: pd.DataFrame([{"latest_bucket": bucket}]),
    )
    monkeypatch.setattr(
        overview,
        "get_health_trend",
        lambda _since: pd.DataFrame(
            [
                {
                    "observation_bucket": bucket,
                    "observed_trip_count": 100,
                    "eligible_trip_count": 100,
                    "late_trip_count": 1,
                    "predicted_late_percentage": 1.0,
                }
            ]
        ),
    )
    monkeypatch.setattr(
        route_reliability,
        "get_period_summary",
        lambda *_args, **_kwargs: pd.DataFrame(
            [
                {
                    "observed_trip_count": 100,
                    "eligible_trip_count": 80,
                    "late_trip_count": 8,
                    "predicted_late_percentage": 10.0,
                    "metric_coverage_percentage": 80.0,
                    "p90_predicted_lateness_minutes": 7.5,
                }
            ]
        ),
    )
    monkeypatch.setattr(
        overview,
        "get_routes_needing_attention",
        lambda *_args, **_kwargs: pd.DataFrame(
            [
                {
                    "route_id": "A",
                    "route_short_name": "A",
                    "direction_id": 0,
                    "eligible_trip_count": 40,
                    "predicted_late_percentage": 25.0,
                    "metric_coverage_percentage": 90.0,
                    "p90_predicted_lateness_minutes": 12.0,
                }
            ]
        ),
    )
    monkeypatch.setattr(
        live_operations,
        "get_active_alerts",
        lambda **_kwargs: pd.DataFrame(
            [
                {
                    "alert_id": "alert-a",
                    "severity": "WARNING",
                    "effect": "DETOUR",
                    "header_text": "Route A detour",
                    "route_ids": ["A"],
                }
            ]
        ),
    )

    page = AppTest.from_file(OVERVIEW_PAGE).run()

    assert not page.exception
    assert [metric.value for metric in page.metric] == ["2", "1", "10.0%", "7.5 min"]
    assert page.get("plotly_chart")
    assert len(page.get("dataframe")) == 2


def test_overview_page_marks_missing_sources_as_unavailable(monkeypatch):
    monkeypatch.setattr(
        overview,
        "get_vehicle_status",
        lambda: pd.DataFrame([{"fresh_vehicle_count": 0, "latest_event_at": None}]),
    )
    monkeypatch.setattr(
        overview,
        "get_alert_status",
        lambda: pd.DataFrame([{"active_alert_count": 0, "latest_feed_at": None}]),
    )
    monkeypatch.setattr(
        overview,
        "get_latest_health_bucket",
        lambda: pd.DataFrame([{"latest_bucket": None}]),
    )
    monkeypatch.setattr(
        overview,
        "get_health_trend",
        lambda _since: pd.DataFrame(
            columns=[
                "observation_bucket",
                "observed_trip_count",
                "eligible_trip_count",
                "late_trip_count",
                "predicted_late_percentage",
            ]
        ),
    )
    monkeypatch.setattr(
        route_reliability, "get_period_summary", lambda *_args, **_kwargs: pd.DataFrame()
    )
    monkeypatch.setattr(
        overview, "get_routes_needing_attention", lambda *_args, **_kwargs: pd.DataFrame()
    )
    monkeypatch.setattr(live_operations, "get_active_alerts", lambda **_kwargs: pd.DataFrame())

    page = AppTest.from_file(OVERVIEW_PAGE).run()

    assert not page.exception
    assert [metric.value for metric in page.metric] == ["0", "—", "—", "—"]
    assert page.warning
    assert page.info


def test_overview_page_does_not_count_alerts_from_stale_feed(monkeypatch):
    old = pd.Timestamp.now(tz="UTC") - pd.Timedelta(minutes=20)
    monkeypatch.setattr(
        overview,
        "get_vehicle_status",
        lambda: pd.DataFrame([{"fresh_vehicle_count": 0, "latest_event_at": old}]),
    )
    monkeypatch.setattr(
        overview,
        "get_alert_status",
        lambda: pd.DataFrame([{"active_alert_count": 3, "latest_feed_at": old}]),
    )
    monkeypatch.setattr(
        overview,
        "get_latest_health_bucket",
        lambda: pd.DataFrame([{"latest_bucket": old}]),
    )
    monkeypatch.setattr(
        overview,
        "get_health_trend",
        lambda _since: pd.DataFrame(
            [
                {
                    "observation_bucket": old,
                    "observed_trip_count": 10,
                    "eligible_trip_count": 10,
                    "late_trip_count": 2,
                    "predicted_late_percentage": 20.0,
                }
            ]
        ),
    )
    monkeypatch.setattr(
        route_reliability,
        "get_period_summary",
        lambda *_args, **_kwargs: pd.DataFrame(
            [
                {
                    "observed_trip_count": 10,
                    "eligible_trip_count": 10,
                    "late_trip_count": 2,
                    "predicted_late_percentage": 20.0,
                    "metric_coverage_percentage": 100.0,
                    "p90_predicted_lateness_minutes": 8.0,
                }
            ]
        ),
    )
    monkeypatch.setattr(
        overview, "get_routes_needing_attention", lambda *_args, **_kwargs: pd.DataFrame()
    )
    monkeypatch.setattr(live_operations, "get_active_alerts", lambda **_kwargs: pd.DataFrame())

    page = AppTest.from_file(OVERVIEW_PAGE).run()

    assert not page.exception
    assert [metric.value for metric in page.metric] == ["0", "—", "20.0%", "8.0 min"]
    assert len(page.warning) == 3

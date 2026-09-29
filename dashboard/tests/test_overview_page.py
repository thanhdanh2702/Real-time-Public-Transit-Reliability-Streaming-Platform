from pathlib import Path

import pandas as pd
from streamlit.testing.v1 import AppTest

from dashboard.queries import overview

OVERVIEW_PAGE = Path(__file__).resolve().parents[1] / "pages/1_overview.py"
APP_PAGE = Path(__file__).resolve().parents[1] / "app.py"


def test_dashboard_entrypoint_opens():
    app = AppTest.from_file(APP_PAGE).run()

    assert not app.exception
    assert app.title[0].value == "TransitPulse"


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

    page = AppTest.from_file(OVERVIEW_PAGE).run()

    assert not page.exception
    assert [metric.value for metric in page.metric] == ["2", "100", "1.0%", "1"]
    assert page.get("plotly_chart")


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

    page = AppTest.from_file(OVERVIEW_PAGE).run()

    assert not page.exception
    assert [metric.value for metric in page.metric] == ["0", "10", "20.0%", "—"]
    assert len(page.warning) == 3

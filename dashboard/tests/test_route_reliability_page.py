from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from streamlit.testing.v1 import AppTest

from dashboard.queries import route_reliability

ROUTE_PAGE = Path(__file__).resolve().parents[1] / "pages/2_route_reliability.py"


def test_route_page_shows_metrics_comparison_and_trip_detail(monkeypatch):
    bucket = pd.Timestamp.now(tz="UTC").floor("5min")
    inspected_buckets = []
    monkeypatch.setattr(
        route_reliability,
        "get_routes",
        lambda: pd.DataFrame(
            [
                {"route_id": "A", "route_short_name": "A", "route_long_name": "Route A"},
                {"route_id": "B", "route_short_name": "B", "route_long_name": "Route B"},
            ]
        ),
    )
    monkeypatch.setattr(
        route_reliability,
        "get_latest_bucket",
        lambda route_id=None, direction_id=None: pd.DataFrame([{"latest_bucket": bucket}]),
    )
    monkeypatch.setattr(
        route_reliability,
        "get_trend",
        lambda since, route_id=None, direction_id=None: pd.DataFrame(
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
        "get_route_comparison",
        lambda since, route_id=None, direction_id=None: pd.DataFrame(
            [
                {
                    "route_id": "A",
                    "route_short_name": "A",
                    "eligible_trip_count": 1,
                    "late_trip_count": 1,
                    "predicted_late_percentage": 100.0,
                },
                {
                    "route_id": "B",
                    "route_short_name": "B",
                    "eligible_trip_count": 99,
                    "late_trip_count": 0,
                    "predicted_late_percentage": 0.0,
                },
            ]
        ),
    )

    def get_trip_samples(since, route_id=None, direction_id=None, bucket=None, limit=100):
        inspected_buckets.append(bucket)
        return pd.DataFrame(
            [
                {
                    "observation_local_timestamp": "2026-09-29 08:00:00",
                    "route_id": "A",
                    "route_short_name": None,
                    "direction_id": 0,
                    "trip_id": "trip-1",
                    "stop_name": "Main St",
                    "delay_basis": "arrival",
                    "scheduled_prediction_timestamp": pd.Timestamp("2026-09-29 12:05:00+00:00"),
                    "delay_prediction_timestamp": pd.Timestamp("2026-09-29 12:11:00+00:00"),
                    "predicted_delay_seconds": 360,
                }
            ]
        )

    monkeypatch.setattr(route_reliability, "get_trip_samples", get_trip_samples)

    page = AppTest.from_file(ROUTE_PAGE).run()

    assert not page.exception
    assert [metric.value for metric in page.metric] == ["100", "100", "1.0%", "100.0%"]
    assert len(page.get("plotly_chart")) == 2
    assert page.get("dataframe")
    assert page.get("dataframe")[0].value.iloc[0]["Route"] == "A"
    next(widget for widget in page.selectbox if widget.label == "Inspect bucket").set_value(
        bucket
    ).run()
    assert inspected_buckets[-1] == bucket


def test_route_page_passes_route_and_direction_filters(monkeypatch):
    bucket = pd.Timestamp.now(tz="UTC")
    calls = []
    monkeypatch.setattr(
        route_reliability,
        "get_routes",
        lambda: pd.DataFrame([{"route_id": "A", "route_short_name": "A", "route_long_name": "A"}]),
    )
    monkeypatch.setattr(
        route_reliability,
        "get_latest_bucket",
        lambda route_id=None, direction_id=None: pd.DataFrame([{"latest_bucket": bucket}]),
    )

    def get_trend(since, route_id=None, direction_id=None):
        calls.append((since, route_id, direction_id))
        return pd.DataFrame(
            [
                {
                    "observation_bucket": bucket,
                    "observed_trip_count": 1,
                    "eligible_trip_count": 1,
                    "late_trip_count": 0,
                    "predicted_late_percentage": 0.0,
                }
            ]
        )

    monkeypatch.setattr(route_reliability, "get_trend", get_trend)
    monkeypatch.setattr(
        route_reliability,
        "get_route_comparison",
        lambda since, route_id=None, direction_id=None: pd.DataFrame(),
    )
    monkeypatch.setattr(
        route_reliability,
        "get_trip_samples",
        lambda since, route_id=None, direction_id=None, bucket=None, limit=100: pd.DataFrame(),
    )

    page = AppTest.from_file(ROUTE_PAGE).run()
    page.sidebar.selectbox[0].set_value("6 hours").run()
    page.sidebar.selectbox[1].set_value("A").run()
    page.sidebar.selectbox[2].set_value(0).run()

    assert not page.exception
    assert calls[-1][1:] == ("A", 0)
    assert 350 < (datetime.now(UTC) - calls[-1][0]).total_seconds() / 60 < 370


def test_route_page_explains_empty_selection(monkeypatch):
    monkeypatch.setattr(route_reliability, "get_routes", lambda: pd.DataFrame(columns=["route_id"]))
    monkeypatch.setattr(
        route_reliability,
        "get_latest_bucket",
        lambda route_id=None, direction_id=None: pd.DataFrame([{"latest_bucket": None}]),
    )
    monkeypatch.setattr(
        route_reliability,
        "get_trend",
        lambda since, route_id=None, direction_id=None: pd.DataFrame(),
    )

    page = AppTest.from_file(ROUTE_PAGE).run()

    assert not page.exception
    assert [metric.value for metric in page.metric] == ["—", "—", "—", "—"]
    assert page.warning
    assert page.info


def test_route_page_warns_when_latest_bucket_is_stale(monkeypatch):
    old = pd.Timestamp.now(tz="UTC") - pd.Timedelta(minutes=20)
    monkeypatch.setattr(route_reliability, "get_routes", lambda: pd.DataFrame(columns=["route_id"]))
    monkeypatch.setattr(
        route_reliability,
        "get_latest_bucket",
        lambda route_id=None, direction_id=None: pd.DataFrame([{"latest_bucket": old}]),
    )
    monkeypatch.setattr(
        route_reliability,
        "get_trend",
        lambda since, route_id=None, direction_id=None: pd.DataFrame(),
    )

    page = AppTest.from_file(ROUTE_PAGE).run()

    assert not page.exception
    assert any("stale" in warning.value for warning in page.warning)


def test_route_page_reports_unavailable_database(monkeypatch):
    def unavailable():
        raise RuntimeError("Missing required environment variable: POSTGRES_PASSWORD")

    monkeypatch.setattr(route_reliability, "get_routes", unavailable)

    page = AppTest.from_file(ROUTE_PAGE).run()

    assert not page.exception
    assert any("PostgreSQL" in error.value for error in page.error)

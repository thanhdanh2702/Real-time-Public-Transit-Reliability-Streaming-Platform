from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

from dashboard.queries import route_reliability

ROUTE_PAGE = Path(__file__).resolve().parents[1] / "pages/2_route_reliability.py"


@pytest.fixture(autouse=True)
def default_period_summary(monkeypatch):
    monkeypatch.setattr(
        route_reliability,
        "get_period_summary",
        lambda *_args, **_kwargs: pd.DataFrame(
            [
                {
                    "observed_trip_count": 100,
                    "eligible_trip_count": 100,
                    "late_trip_count": 1,
                    "predicted_late_percentage": 1.0,
                    "metric_coverage_percentage": 100.0,
                    "p90_predicted_lateness_minutes": 6.0,
                }
            ]
        ),
    )


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
        lambda since, route_id=None, direction_id=None, **_kwargs: pd.DataFrame(
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
        lambda since, route_id=None, direction_id=None, **_kwargs: pd.DataFrame(
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

    def get_trip_samples(
        since, route_id=None, direction_id=None, bucket=None, limit=100, **_kwargs
    ):
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
    assert [metric.value for metric in page.metric] == [
        "100",
        "100",
        "1.0%",
        "100.0%",
        "6.0 min",
    ]
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

    def get_trend(since, route_id=None, direction_id=None, **_kwargs):
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
        lambda since, route_id=None, direction_id=None, **_kwargs: pd.DataFrame(),
    )
    monkeypatch.setattr(
        route_reliability,
        "get_trip_samples",
        lambda since, route_id=None, direction_id=None, bucket=None, limit=100, **_kwargs: (
            pd.DataFrame()
        ),
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
        lambda since, route_id=None, direction_id=None, **_kwargs: pd.DataFrame(),
    )
    monkeypatch.setattr(
        route_reliability,
        "get_period_summary",
        lambda *_args, **_kwargs: pd.DataFrame(
            [
                {
                    "observed_trip_count": 0,
                    "eligible_trip_count": 0,
                    "late_trip_count": 0,
                    "predicted_late_percentage": None,
                    "metric_coverage_percentage": None,
                    "p90_predicted_lateness_minutes": None,
                }
            ]
        ),
    )

    page = AppTest.from_file(ROUTE_PAGE).run()

    assert not page.exception
    assert [metric.value for metric in page.metric] == ["0", "0", "—", "—", "—"]
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
        lambda since, route_id=None, direction_id=None, **_kwargs: pd.DataFrame(),
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


def test_route_page_hides_pandas_database_error_in_filters(monkeypatch):
    def unavailable():
        raise pd.errors.DatabaseError("POSTGRES_PASSWORD=secret")

    monkeypatch.setattr(route_reliability, "get_routes", unavailable)

    page = AppTest.from_file(ROUTE_PAGE).run()

    assert not page.exception
    assert [error.value for error in page.error] == [
        "Could not load route data from PostgreSQL. Check the database and dbt marts."
    ]


def test_route_page_hides_pandas_database_error_in_trip_detail(monkeypatch):
    bucket = pd.Timestamp.now(tz="UTC").floor("5min")
    monkeypatch.setattr(route_reliability, "get_routes", lambda: pd.DataFrame(columns=["route_id"]))
    monkeypatch.setattr(
        route_reliability,
        "get_latest_bucket",
        lambda route_id=None, direction_id=None: pd.DataFrame([{"latest_bucket": bucket}]),
    )
    monkeypatch.setattr(
        route_reliability,
        "get_trend",
        lambda since, route_id=None, direction_id=None, **_kwargs: pd.DataFrame(
            [
                {
                    "observation_bucket": bucket,
                    "observed_trip_count": 1,
                    "eligible_trip_count": 1,
                    "late_trip_count": 0,
                    "predicted_late_percentage": 0.0,
                }
            ]
        ),
    )
    monkeypatch.setattr(
        route_reliability,
        "get_route_comparison",
        lambda since, route_id=None, direction_id=None, **_kwargs: pd.DataFrame(
            columns=["predicted_late_percentage"]
        ),
    )

    def unavailable(*_args, **_kwargs):
        raise pd.errors.DatabaseError("POSTGRES_PASSWORD=secret")

    monkeypatch.setattr(route_reliability, "get_trip_samples", unavailable)

    page = AppTest.from_file(ROUTE_PAGE).run()

    assert not page.exception
    assert [error.value for error in page.error] == ["Could not load trip samples from PostgreSQL."]


def test_route_page_shows_sample_p90_previous_delta_and_sort_control(monkeypatch):
    bucket = pd.Timestamp.now(tz="UTC").floor("5min")
    period_calls = []
    sample_sorts = []
    monkeypatch.setattr(
        route_reliability,
        "get_routes",
        lambda: pd.DataFrame(
            [{"route_id": "A", "route_short_name": "A", "route_long_name": "Route A"}]
        ),
    )
    monkeypatch.setattr(
        route_reliability,
        "get_latest_bucket",
        lambda **_kwargs: pd.DataFrame([{"latest_bucket": bucket}]),
    )
    monkeypatch.setattr(
        route_reliability,
        "get_trend",
        lambda *_args, **_kwargs: pd.DataFrame(
            [
                {
                    "observation_bucket": bucket,
                    "observed_trip_count": 100,
                    "eligible_trip_count": 80,
                    "late_trip_count": 8,
                    "predicted_late_percentage": 10.0,
                }
            ]
        ),
    )

    def period(start, end, **_kwargs):
        period_calls.append((start, end))
        return pd.DataFrame(
            [
                {
                    "observed_trip_count": 100 if len(period_calls) == 1 else 80,
                    "eligible_trip_count": 80 if len(period_calls) == 1 else 70,
                    "late_trip_count": 8 if len(period_calls) == 1 else 7,
                    "predicted_late_percentage": 10.0,
                    "metric_coverage_percentage": 80.0 if len(period_calls) == 1 else 87.5,
                    "p90_predicted_lateness_minutes": 7.5 if len(period_calls) == 1 else 6.0,
                }
            ]
        )

    monkeypatch.setattr(route_reliability, "get_period_summary", period)
    monkeypatch.setattr(
        route_reliability,
        "get_route_comparison",
        lambda *_args, **_kwargs: pd.DataFrame(),
    )

    def samples(*_args, sort_by="latest", **_kwargs):
        sample_sorts.append(sort_by)
        return pd.DataFrame()

    monkeypatch.setattr(route_reliability, "get_trip_samples", samples)

    page = AppTest.from_file(ROUTE_PAGE).run()

    assert not page.exception
    assert [metric.value for metric in page.metric] == [
        "100",
        "80",
        "10.0%",
        "80.0%",
        "7.5 min",
    ]
    assert page.metric[-1].delta == "1.5 min"
    assert period_calls[0][1] - period_calls[0][0] == period_calls[1][1] - period_calls[1][0]
    next(widget for widget in page.selectbox if widget.label == "Sort samples").set_value(
        "Largest predicted delay"
    ).run()
    assert sample_sorts[-1] == "largest_delay"

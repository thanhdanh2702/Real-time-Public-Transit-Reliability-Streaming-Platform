import pandas as pd

from dashboard.components.charts import predicted_late_trend_chart, route_comparison_chart


def test_predicted_late_chart_uses_project_local_time(monkeypatch):
    monkeypatch.setenv("APP_TIMEZONE", "America/New_York")
    trend = pd.DataFrame(
        {
            "observation_bucket": [pd.Timestamp("2026-09-29 12:00:00+00:00")],
            "predicted_late_percentage": [10.0],
        }
    )

    chart = predicted_late_trend_chart(trend)

    assert str(chart.data[0].x[0]).startswith("2026-09-29T08:00")
    assert list(chart.data[0].y) == [10.0]


def test_route_comparison_keeps_routes_distinct_when_names_repeat():
    comparison = pd.DataFrame(
        {
            "route_id": ["A", "B"],
            "route_short_name": ["Same", "Same"],
            "eligible_trip_count": [10, 20],
            "late_trip_count": [2, 1],
            "predicted_late_percentage": [20.0, 5.0],
        }
    )

    chart = route_comparison_chart(comparison)

    assert list(chart.data[0].x) == ["A", "B"]

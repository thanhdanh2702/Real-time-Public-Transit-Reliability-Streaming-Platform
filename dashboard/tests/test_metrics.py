import pandas as pd

from dashboard.components.metrics import (
    metric_delta,
    qualified_comparison_value,
    summarize_reliability,
)


def test_reliability_rate_uses_total_late_and_eligible_counts():
    trend = pd.DataFrame(
        {
            "observed_trip_count": [1, 99],
            "eligible_trip_count": [1, 99],
            "late_trip_count": [1, 0],
        }
    )

    summary = summarize_reliability(trend)

    assert summary == {
        "observed": 100,
        "eligible": 100,
        "late": 1,
        "late_percentage": 1.0,
        "coverage_percentage": 100.0,
    }


def test_reliability_rate_is_unknown_without_eligible_samples():
    trend = pd.DataFrame(
        {"observed_trip_count": [2], "eligible_trip_count": [0], "late_trip_count": [0]}
    )

    assert summarize_reliability(trend)["late_percentage"] is None
    assert summarize_reliability(trend)["coverage_percentage"] == 0.0
    assert summarize_reliability(trend.iloc[0:0])["observed"] is None


def test_metric_delta_is_unknown_when_previous_period_is_missing():
    assert metric_delta(12.5, None) is None
    assert metric_delta(None, 10.0) is None
    assert metric_delta(12.5, 10.0) == 2.5


def test_comparison_requires_enough_samples_in_both_periods():
    assert (
        qualified_comparison_value(
            12.5,
            current_samples=30,
            previous_samples=25,
            minimum_samples=20,
        )
        == 12.5
    )
    assert (
        qualified_comparison_value(
            12.5,
            current_samples=30,
            previous_samples=5,
            minimum_samples=20,
        )
        is None
    )

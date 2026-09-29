import pandas as pd

from dashboard.components.metrics import summarize_reliability


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
    }


def test_reliability_rate_is_unknown_without_eligible_samples():
    trend = pd.DataFrame(
        {"observed_trip_count": [2], "eligible_trip_count": [0], "late_trip_count": [0]}
    )

    assert summarize_reliability(trend)["late_percentage"] is None
    assert summarize_reliability(trend.iloc[0:0])["observed"] is None

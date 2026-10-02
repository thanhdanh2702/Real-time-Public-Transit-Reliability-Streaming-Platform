import pandas as pd


def metric_delta(current: float | None, previous: float | None) -> float | None:
    """Return a percentage-point/minute delta only when both periods exist."""
    if current is None or previous is None or pd.isna(current) or pd.isna(previous):
        return None
    return round(float(current) - float(previous), 1)


def qualified_comparison_value(
    value: float | None,
    *,
    current_samples: float | None,
    previous_samples: float | None,
    minimum_samples: int,
) -> float | None:
    """Return a prior-period metric only when both windows have enough eligible samples."""
    if value is None or pd.isna(value):
        return None
    if current_samples is None or previous_samples is None:
        return None
    if current_samples < minimum_samples or previous_samples < minimum_samples:
        return None
    return float(value)


def summarize_reliability(trend: pd.DataFrame) -> dict[str, int | float | None]:
    if trend.empty:
        return {
            "observed": None,
            "eligible": None,
            "late": None,
            "late_percentage": None,
            "coverage_percentage": None,
        }

    observed = int(trend["observed_trip_count"].sum())
    eligible = int(trend["eligible_trip_count"].sum())
    late = int(trend["late_trip_count"].sum())
    return {
        "observed": observed,
        "eligible": eligible,
        "late": late,
        "late_percentage": round(100 * late / eligible, 1) if eligible else None,
        "coverage_percentage": round(100 * eligible / observed, 1) if observed else None,
    }

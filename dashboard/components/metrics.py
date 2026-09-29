import pandas as pd


def summarize_reliability(trend: pd.DataFrame) -> dict[str, int | float | None]:
    if trend.empty:
        return {"observed": None, "eligible": None, "late": None, "late_percentage": None}

    observed = int(trend["observed_trip_count"].sum())
    eligible = int(trend["eligible_trip_count"].sum())
    late = int(trend["late_trip_count"].sum())
    return {
        "observed": observed,
        "eligible": eligible,
        "late": late,
        "late_percentage": round(100 * late / eligible, 1) if eligible else None,
    }

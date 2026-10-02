import os
from datetime import UTC, datetime, timedelta

import pandas as pd
import streamlit as st
from sqlalchemy.exc import SQLAlchemyError

from dashboard.components.charts import reliability_trend_chart, route_comparison_chart
from dashboard.components.filters import route_reliability_filters
from dashboard.components.metrics import metric_delta, qualified_comparison_value
from dashboard.queries import route_reliability

st.set_page_config(page_title="TransitPulse | Route Reliability", page_icon="📈", layout="wide")
st.title("Route reliability")
st.caption(
    "MBTA bus trip observation samples · predicted schedule delay · "
    "times shown in America/New_York"
)


def _local_time(value: object) -> str:
    timezone = os.getenv("APP_TIMEZONE", "America/New_York")
    return pd.to_datetime(value, utc=True).tz_convert(timezone).strftime("%Y-%m-%d %H:%M %Z")


def _display_trip_samples(details: pd.DataFrame) -> pd.DataFrame:
    timezone = os.getenv("APP_TIMEZONE", "America/New_York")

    def local_column(name: str) -> pd.Series:
        return (
            pd.to_datetime(details[name], utc=True, errors="coerce")
            .dt.tz_convert(timezone)
            .dt.strftime("%Y-%m-%d %H:%M")
        )

    return pd.DataFrame(
        {
            "Observed": pd.to_datetime(details["observation_local_timestamp"]).dt.strftime(
                "%Y-%m-%d %H:%M"
            ),
            "Route": details["route_short_name"].fillna(details["route_id"]),
            "Direction": details["direction_id"].replace({-1: "Unknown"}),
            "Trip": details["trip_id"],
            "Next stop": details["stop_name"],
            "Basis": details["delay_basis"],
            "Scheduled": local_column("scheduled_prediction_timestamp"),
            "Predicted": local_column("delay_prediction_timestamp"),
            "Offset (min)": (details["predicted_delay_seconds"] / 60).round(1),
        }
    )


def _period_row(rows: pd.DataFrame) -> pd.Series | None:
    return rows.iloc[0] if not rows.empty else None


def _number(row: pd.Series | None, name: str) -> float | None:
    if row is None or name not in row or pd.isna(row[name]):
        return None
    return float(row[name])


def _delta_label(value: float | None, suffix: str = "") -> str | None:
    if value is None:
        return None
    return f"{value:.1f}{suffix}"


@st.fragment(run_every="30s")
def show_route_reliability() -> None:
    try:
        minutes, route_id, direction_id, min_samples, sort_by = route_reliability_filters(
            route_reliability.get_routes()
        )
        end_time = datetime.now(UTC)
        start_time = end_time - timedelta(minutes=minutes)
        previous_start = start_time - timedelta(minutes=minutes)

        latest_row = route_reliability.get_latest_bucket(
            route_id=route_id,
            direction_id=direction_id,
        ).iloc[0]
        latest = latest_row["latest_bucket"]
        trend = route_reliability.get_trend(
            start_time,
            route_id,
            direction_id,
            until=end_time,
        )
        current = _period_row(
            route_reliability.get_period_summary(
                start_time,
                end_time,
                route_id=route_id,
                direction_id=direction_id,
            )
        )
        previous = _period_row(
            route_reliability.get_period_summary(
                previous_start,
                start_time,
                route_id=route_id,
                direction_id=direction_id,
            )
        )
        comparison = (
            route_reliability.get_route_comparison(
                start_time,
                route_id,
                direction_id,
                until=end_time,
                min_eligible_samples=min_samples,
            )
            if route_id is None and not trend.empty
            else pd.DataFrame()
        )
    except (SQLAlchemyError, pd.errors.DatabaseError, RuntimeError):
        st.error("Could not load route data from PostgreSQL. Check the database and dbt marts.")
        return

    if pd.isna(latest):
        st.warning("No trip monitoring data for this route and direction.")
    else:
        st.caption(f"Last observed bucket: {_local_time(latest)}")
        if pd.Timestamp.now(tz="UTC") - pd.to_datetime(latest, utc=True) > pd.Timedelta(minutes=15):
            st.warning("Trip monitoring data is stale. Run dbt build after new Spark data arrives.")

    observed_value = _number(current, "observed_trip_count")
    eligible_value = _number(current, "eligible_trip_count")
    late_value = _number(current, "predicted_late_percentage")
    coverage_value = _number(current, "metric_coverage_percentage")
    p90_value = _number(current, "p90_predicted_lateness_minutes")

    previous_samples = _number(previous, "eligible_trip_count")
    previous_late = qualified_comparison_value(
        _number(previous, "predicted_late_percentage"),
        current_samples=eligible_value,
        previous_samples=previous_samples,
        minimum_samples=min_samples,
    )
    previous_coverage = qualified_comparison_value(
        _number(previous, "metric_coverage_percentage"),
        current_samples=eligible_value,
        previous_samples=previous_samples,
        minimum_samples=min_samples,
    )
    previous_p90 = qualified_comparison_value(
        _number(previous, "p90_predicted_lateness_minutes"),
        current_samples=eligible_value,
        previous_samples=previous_samples,
        minimum_samples=min_samples,
    )

    observed, eligible, late_rate, coverage, p90 = st.columns(5)
    observed.metric("Observed samples", int(observed_value) if observed_value is not None else "—")
    eligible.metric("Eligible samples", int(eligible_value) if eligible_value is not None else "—")
    late_rate.metric(
        "Predicted-late rate",
        f"{late_value:.1f}%" if late_value is not None else "—",
        delta=_delta_label(metric_delta(late_value, previous_late), " pp"),
        delta_color="inverse",
    )
    coverage.metric(
        "Metric coverage",
        f"{coverage_value:.1f}%" if coverage_value is not None else "—",
        delta=_delta_label(metric_delta(coverage_value, previous_coverage), " pp"),
    )
    p90.metric(
        "P90 predicted lateness",
        f"{p90_value:.1f} min" if p90_value is not None else "—",
        delta=_delta_label(metric_delta(p90_value, previous_p90), " min"),
        delta_color="inverse",
    )
    st.caption(
        "Predicted late = more than five minutes after schedule. "
        "Late rate = late / eligible; coverage = eligible / observed. "
        "Deltas compare with the immediately preceding window of equal length. "
        f"Both windows need at least {min_samples} eligible samples for a delta. "
        "A trip may appear in multiple observation buckets."
    )

    if trend.empty:
        st.info("No bus trip samples in the selected time window.")
        return

    st.subheader("Predicted lateness and sample volume")
    st.plotly_chart(reliability_trend_chart(trend), width="stretch")

    if route_id is None:
        comparable = (
            comparison.dropna(subset=["predicted_late_percentage"])
            if "predicted_late_percentage" in comparison
            else pd.DataFrame()
        )
        if len(comparable) > 1:
            st.subheader("Route comparison")
            st.caption(
                f"Only routes with at least {min_samples} eligible observation samples are shown."
            )
            st.plotly_chart(route_comparison_chart(comparable), width="stretch")
        elif not comparable.empty:
            st.info("Only one route meets the selected sample threshold.")

    st.subheader("Trip observation samples")
    bucket_options = [None, *trend["observation_bucket"].iloc[::-1].tolist()]
    selected_bucket = st.selectbox(
        "Inspect bucket",
        bucket_options,
        format_func=lambda value: "All buckets in window" if value is None else _local_time(value),
    )
    try:
        details = route_reliability.get_trip_samples(
            start_time,
            route_id,
            direction_id,
            bucket=selected_bucket,
            limit=100,
            sort_by=sort_by,
        )
    except (SQLAlchemyError, pd.errors.DatabaseError, RuntimeError):
        st.error("Could not load trip samples from PostgreSQL.")
        return
    if details.empty:
        st.info("No trip observation samples match this selection.")
    else:
        st.caption(
            "Positive offset means predicted late; negative offset means predicted early. "
            "Rows are observations, not unique completed trips."
        )
        st.dataframe(_display_trip_samples(details), hide_index=True, width="stretch")


show_route_reliability()

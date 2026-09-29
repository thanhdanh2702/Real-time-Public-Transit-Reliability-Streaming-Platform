import os
from datetime import UTC, datetime, timedelta

import pandas as pd
import streamlit as st
from sqlalchemy.exc import SQLAlchemyError

from dashboard.components.charts import predicted_late_trend_chart, route_comparison_chart
from dashboard.components.filters import route_reliability_filters
from dashboard.components.metrics import summarize_reliability
from dashboard.queries import route_reliability

st.set_page_config(page_title="TransitPulse | Route Reliability", layout="wide")
st.title("Route reliability")
st.caption("MBTA bus trip samples · predicted delay compared with the GTFS schedule")


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
            "Direction": details["direction_id"],
            "Trip": details["trip_id"],
            "Next stop": details["stop_name"],
            "Basis": details["delay_basis"],
            "Scheduled": local_column("scheduled_prediction_timestamp"),
            "Predicted": local_column("delay_prediction_timestamp"),
            "Offset (min)": (details["predicted_delay_seconds"] / 60).round(1),
        }
    )


@st.fragment(run_every="30s")
def show_route_reliability() -> None:
    try:
        minutes, route_id, direction_id = route_reliability_filters(route_reliability.get_routes())
        since = datetime.now(UTC) - timedelta(minutes=minutes)
        latest_row = route_reliability.get_latest_bucket(route_id, direction_id).iloc[0]
        latest = latest_row["latest_bucket"]
        trend = route_reliability.get_trend(since, route_id, direction_id)
        comparison = (
            route_reliability.get_route_comparison(since, route_id, direction_id)
            if route_id is None and not trend.empty
            else pd.DataFrame()
        )
    except (SQLAlchemyError, RuntimeError):
        st.error("Could not load route data from PostgreSQL. Check the database and dbt marts.")
        return

    summary = summarize_reliability(trend)

    if pd.isna(latest):
        st.warning("No trip monitoring data for this route and direction.")
    else:
        st.caption(f"Last observed bucket: {_local_time(latest)}")
        if pd.Timestamp.now(tz="UTC") - pd.to_datetime(latest, utc=True) > pd.Timedelta(minutes=15):
            st.warning("Trip monitoring data is stale. Run dbt build after new Spark data arrives.")

    observed, eligible, late_rate, coverage = st.columns(4)
    observed_value = summary["observed"]
    eligible_value = summary["eligible"]
    observed.metric("Observed trip samples", observed_value if observed_value is not None else "—")
    eligible.metric("Eligible samples", eligible_value if eligible_value is not None else "—")
    late_value = summary["late_percentage"]
    coverage_value = summary["coverage_percentage"]
    late_rate.metric("Predicted late", f"{late_value:.1f}%" if late_value is not None else "—")
    coverage_label = f"{coverage_value:.1f}%" if coverage_value is not None else "—"
    coverage.metric("Metric coverage", coverage_label)
    st.caption(
        "Predicted late = more than 5 minutes after schedule. "
        "Late rate = late / eligible; coverage = eligible / observed. "
        f"Late samples: {summary['late'] if summary['late'] is not None else '—'}."
    )

    if trend.empty:
        st.info("No bus trip samples in the selected time window.")
        return

    st.subheader("Predicted late over time")
    st.plotly_chart(predicted_late_trend_chart(trend), use_container_width=True)

    if route_id is None:
        comparable = comparison.dropna(subset=["predicted_late_percentage"])
        if len(comparable) > 1:
            st.subheader("Routes by predicted-late share")
            st.caption("Top 10 routes by rate; check eligible sample counts in the chart tooltip.")
            st.plotly_chart(route_comparison_chart(comparable), use_container_width=True)

    st.subheader("Trip samples")
    bucket_options = [None, *trend["observation_bucket"].iloc[::-1].tolist()]
    selected_bucket = st.selectbox(
        "Inspect bucket",
        bucket_options,
        format_func=lambda value: "Latest 100 samples" if value is None else _local_time(value),
    )
    try:
        details = route_reliability.get_trip_samples(
            since, route_id, direction_id, bucket=selected_bucket, limit=100
        )
    except (SQLAlchemyError, RuntimeError):
        st.error("Could not load trip samples from PostgreSQL.")
        return
    if details.empty:
        st.info("No trip samples match this selection.")
    else:
        st.caption("Positive offset means predicted late; negative offset means predicted early.")
        st.dataframe(_display_trip_samples(details), hide_index=True, use_container_width=True)


show_route_reliability()

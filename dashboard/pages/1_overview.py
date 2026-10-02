import os
from datetime import UTC, datetime, timedelta

import pandas as pd
import streamlit as st
from sqlalchemy.exc import SQLAlchemyError

from dashboard.components.charts import reliability_trend_chart
from dashboard.queries import live_operations, overview, route_reliability

WINDOW_MINUTES = 60

st.set_page_config(page_title="TransitPulse | Overview", page_icon="🚌", layout="wide")
st.title("TransitPulse · Overview")
st.caption(
    "MBTA bus operations · rolling 60-minute window · UI refreshes every 30 seconds · "
    "America/New_York"
)


def _show_source_status(name: str, observed_at: object, max_age_minutes: float) -> bool:
    if pd.isna(observed_at):
        st.warning(f"{name}: no data available.")
        return False

    timestamp = pd.to_datetime(observed_at, utc=True)
    local_time = timestamp.tz_convert(os.getenv("APP_TIMEZONE", "America/New_York"))
    st.caption(f"{name} last observed: {local_time:%Y-%m-%d %H:%M:%S %Z}")
    if pd.Timestamp.now(tz="UTC") - timestamp > pd.Timedelta(minutes=max_age_minutes):
        st.warning(f"{name}: data is stale.")
        return False
    return True


def _value(row: pd.Series | None, name: str) -> object | None:
    if row is None or name not in row or pd.isna(row[name]):
        return None
    return row[name]


def _route_attention_display(routes: pd.DataFrame) -> pd.DataFrame:
    if routes.empty:
        return routes
    result = routes.copy()
    result["Route"] = result["route_short_name"].fillna(result["route_id"])
    result["Direction"] = result["direction_id"].replace({-1: "Unknown"})
    result["Predicted late"] = result["predicted_late_percentage"].map(
        lambda value: f"{value:.1f}%"
    )
    result["P90 lateness"] = result["p90_predicted_lateness_minutes"].map(
        lambda value: f"{value:.1f} min"
    )
    result["Coverage"] = result["metric_coverage_percentage"].map(
        lambda value: f"{value:.1f}%"
    )
    return result[
        ["Route", "Direction", "Predicted late", "P90 lateness", "eligible_trip_count", "Coverage"]
    ].rename(columns={"eligible_trip_count": "Eligible samples"})


def _alert_display(alerts: pd.DataFrame) -> pd.DataFrame:
    if alerts.empty:
        return alerts
    result = alerts.reindex(columns=["severity", "effect", "header_text", "route_ids"]).copy()
    result["severity"] = result["severity"].fillna("Unknown")
    result["effect"] = result["effect"].fillna("Unknown")
    result["header_text"] = result["header_text"].fillna("No heading")
    result["route_ids"] = result["route_ids"].apply(
        lambda values: ", ".join(str(value) for value in values)
        if isinstance(values, (list, tuple)) and values
        else "Route not specified"
    )
    return result.rename(
        columns={
            "severity": "Severity",
            "effect": "Effect",
            "header_text": "Message",
            "route_ids": "Routes",
        }
    )


@st.fragment(run_every="30s")
def show_overview() -> None:
    min_samples = st.sidebar.slider(
        "Minimum eligible samples",
        min_value=1,
        max_value=200,
        value=20,
        help="Applied to the routes-needing-attention table.",
    )
    end_time = datetime.now(UTC)
    start_time = end_time - timedelta(minutes=WINDOW_MINUTES)

    try:
        vehicle = overview.get_vehicle_status().iloc[0]
        alerts_status = overview.get_alert_status().iloc[0]
        latest_health = overview.get_latest_health_bucket().iloc[0]
        trend = overview.get_health_trend(start_time)
        period_rows = route_reliability.get_period_summary(start_time, end_time)
        period = period_rows.iloc[0] if not period_rows.empty else None
        attention = overview.get_routes_needing_attention(
            start_time,
            end_time,
            min_eligible_samples=min_samples,
        )
        alerts = live_operations.get_active_alerts(limit=5)
    except (SQLAlchemyError, pd.errors.DatabaseError, RuntimeError):
        st.error("Could not load overview data from PostgreSQL. Check the database and dbt marts.")
        return

    st.subheader("Data freshness")
    _show_source_status("Vehicle positions", vehicle["latest_event_at"], 1.5)
    alerts_fresh = _show_source_status("Service alerts", alerts_status["latest_feed_at"], 10)
    _show_source_status("Trip monitoring", latest_health["latest_bucket"], 15)

    vehicle_count = int(vehicle["fresh_vehicle_count"])
    alert_count = int(alerts_status["active_alert_count"]) if alerts_fresh else "—"
    late_rate = _value(period, "predicted_late_percentage")
    p90 = _value(period, "p90_predicted_lateness_minutes")

    vehicle_col, alert_col, late_col, p90_col = st.columns(4)
    vehicle_col.metric("Fresh bus vehicles", vehicle_count)
    alert_col.metric("Active service alerts", alert_count)
    late_col.metric("Predicted-late rate", f"{late_rate:.1f}%" if late_rate is not None else "—")
    p90_col.metric("P90 predicted lateness", f"{p90:.1f} min" if p90 is not None else "—")

    observed = _value(period, "observed_trip_count")
    eligible = _value(period, "eligible_trip_count")
    coverage = _value(period, "metric_coverage_percentage")
    coverage_text = f"{coverage:.1f}%" if coverage is not None else "—"
    st.caption(
        "Predicted late means more than five minutes after the GTFS schedule. "
        f"Observed samples: {int(observed) if observed is not None else '—'} · "
        f"Eligible samples: {int(eligible) if eligible is not None else '—'} · "
        f"Coverage: {coverage_text}"
    )

    st.subheader("Reliability over time")
    if trend.empty:
        st.info("No bus trip samples in the last 60 minutes.")
    else:
        st.plotly_chart(reliability_trend_chart(trend), width="stretch")

    route_col, alert_summary_col = st.columns(2)
    with route_col:
        st.subheader("Routes needing attention")
        st.caption(
            f"Only route-directions with at least {min_samples} eligible samples are ranked."
        )
        if attention.empty:
            st.info("No route has enough eligible samples in this window.")
        else:
            st.dataframe(_route_attention_display(attention), hide_index=True, width="stretch")

    with alert_summary_col:
        st.subheader("Service alert summary")
        if not alerts_fresh:
            st.info("Current alert status is unknown because the stored snapshot is stale.")
        elif alerts.empty:
            st.info("No alerts are active in the latest stored non-empty snapshot.")
        else:
            st.dataframe(_alert_display(alerts), hide_index=True, width="stretch")
        st.caption("A newer empty alert snapshot is not observable with the current source model.")


show_overview()

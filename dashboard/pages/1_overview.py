import os
from datetime import UTC, datetime, timedelta

import pandas as pd
import streamlit as st

from dashboard.components.charts import predicted_late_trend_chart
from dashboard.components.metrics import summarize_reliability
from dashboard.queries import overview

st.set_page_config(page_title="TransitPulse | Overview", layout="wide")
st.title("TransitPulse · Overview")
st.caption("MBTA bus operations · last 60 minutes · refreshes every 30 seconds")


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


@st.fragment(run_every="30s")
def show_overview() -> None:
    vehicle = overview.get_vehicle_status().iloc[0]
    alerts = overview.get_alert_status().iloc[0]
    latest_health = overview.get_latest_health_bucket().iloc[0]
    trend = overview.get_health_trend(datetime.now(UTC) - timedelta(minutes=60))
    reliability = summarize_reliability(trend)

    st.subheader("Data freshness")
    _show_source_status("Vehicle positions", vehicle["latest_event_at"], 1.5)
    alerts_fresh = _show_source_status("Service alerts", alerts["latest_feed_at"], 10)
    _show_source_status("Trip monitoring", latest_health["latest_bucket"], 15)

    vehicle_col, samples_col, late_col, alert_col = st.columns(4)
    vehicle_col.metric("Fresh vehicles", int(vehicle["fresh_vehicle_count"]))
    observed = reliability["observed"]
    samples_col.metric("Observed trip samples", observed if observed is not None else "—")
    late_percentage = reliability["late_percentage"]
    late_label = f"{late_percentage:.1f}%" if late_percentage is not None else "—"
    late_col.metric("Predicted late", late_label)
    alert_count = int(alerts["active_alert_count"]) if alerts_fresh else "—"
    alert_col.metric("Active service alerts", alert_count)

    eligible = reliability["eligible"]
    st.caption(
        "Predicted late: more than 5 minutes after schedule. "
        "Rate = late samples / eligible samples. "
        f"Eligible samples: {eligible if eligible is not None else '—'}."
    )
    st.subheader("Predicted late by 5-minute bucket")
    if trend.empty:
        st.info("No bus trip samples in the last 60 minutes.")
    else:
        st.plotly_chart(predicted_late_trend_chart(trend), use_container_width=True)


show_overview()

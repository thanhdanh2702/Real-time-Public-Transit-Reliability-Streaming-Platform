import os

import pandas as pd
import streamlit as st
from sqlalchemy.exc import SQLAlchemyError

from dashboard.components.vehicle_map import build_vehicle_map
from dashboard.queries import live_operations, route_reliability

VEHICLE_LIMIT = 2000
ALERT_LIMIT = 100

st.set_page_config(page_title="TransitPulse | Live Operations", layout="wide")
st.title("Live Operations")
st.caption("MBTA bus positions and service alerts · refreshes every 30 seconds")


def _local_time(value: object) -> str:
    timezone = os.getenv("APP_TIMEZONE", "America/New_York")
    return pd.to_datetime(value, utc=True).tz_convert(timezone).strftime("%Y-%m-%d %H:%M:%S %Z")


def _route_labels(routes: pd.DataFrame) -> dict[str, str]:
    labels = {}
    for _, row in routes.iterrows():
        route_id = str(row["route_id"])
        short_name = row["route_short_name"]
        if not isinstance(short_name, str) or not short_name:
            short_name = route_id
        long_name = row["route_long_name"]
        labels[route_id] = (
            f"{short_name} · {long_name}"
            if isinstance(long_name, str) and long_name
            else short_name
        )
    return labels


def _route_scope(route_ids: object) -> str:
    if isinstance(route_ids, (list, tuple)) and route_ids:
        return ", ".join(str(route_id) for route_id in route_ids)
    return "Route not specified"


def _text(value: object, missing: str = "—") -> str:
    if value is None or pd.isna(value) or value == "":
        return missing
    return str(value)


@st.fragment(run_every="30s")
def show_live_operations() -> None:
    try:
        labels = _route_labels(route_reliability.get_routes())
        route_id = st.sidebar.selectbox(
            "Route",
            [None, *labels],
            format_func=lambda value: "All bus routes" if value is None else labels[value],
        )
        vehicle_status = live_operations.get_vehicle_feed_status().iloc[0]
        alert_status = live_operations.get_alert_feed_status().iloc[0]
        vehicle_rows = live_operations.get_live_vehicles(route_id=route_id, limit=VEHICLE_LIMIT)
        alert_rows = live_operations.get_active_alerts(route_id=route_id, limit=ALERT_LIMIT)
    except (SQLAlchemyError, pd.errors.DatabaseError, RuntimeError):
        st.error(
            "Could not load live operations from PostgreSQL. Check the database and dbt marts."
        )
        return

    vehicles_truncated = len(vehicle_rows) > VEHICLE_LIMIT
    alerts_truncated = len(alert_rows) > ALERT_LIMIT
    vehicles = vehicle_rows.head(VEHICLE_LIMIT)
    alerts = alert_rows.head(ALERT_LIMIT)
    vehicle_fresh = bool(vehicle_status["fresh_vehicle_count"])
    alert_fresh = bool(alert_status["fresh_snapshot_entities"])

    if pd.isna(vehicle_status["latest_event_at"]):
        st.warning("No bus position has been stored yet.")
    else:
        st.caption(f"Bus position last observed: {_local_time(vehicle_status['latest_event_at'])}")
        if not vehicle_fresh:
            st.warning("No fresh bus positions are available; old positions are hidden.")

    if pd.isna(alert_status["latest_feed_at"]):
        st.warning("No stored service-alert snapshot is available; alert status is unknown.")
    else:
        st.caption(f"Alert feed last observed: {_local_time(alert_status['latest_feed_at'])}")
        if not alert_fresh:
            st.warning("The stored alert snapshot is stale; current alert status is unknown.")

    vehicle_metric, alert_metric = st.columns(2)
    vehicle_metric.metric("Vehicles on map", len(vehicles))
    alert_metric.metric("Alerts shown", len(alerts) if alert_fresh else "—")

    st.subheader("Bus positions")
    if vehicles_truncated:
        st.warning(f"Showing only the first {VEHICLE_LIMIT} fresh vehicles.")
    if vehicles.empty:
        st.info("No fresh bus positions with usable coordinates match the selected route.")
    else:
        vehicle_ids = vehicles["vehicle_id"].astype(str).tolist()
        selected_id = st.selectbox("Vehicle", vehicle_ids)
        selected = vehicles.loc[vehicles["vehicle_id"].astype(str) == selected_id].iloc[0]
        deck = build_vehicle_map(vehicles, selected_vehicle_id=selected_id)
        if deck is not None:
            st.pydeck_chart(deck, height=520, width="stretch")
        route_name = _text(selected["route_short_name"], _text(selected["route_id"]))
        st.write(f"Route: {route_name}")
        st.write(f"Trip: {_text(selected['trip_id'])}")
        st.write(f"Head sign: {_text(selected['trip_headsign'])}")
        st.write(f"Direction: {_text(selected['direction_id'])}")
        st.write(f"Occupancy: {_text(selected['occupancy_status'])}")
        st.caption(f"Vehicle last observed: {_local_time(selected['event_timestamp'])}")

    st.subheader("Service alerts")
    if route_id is not None:
        st.caption(
            "Alerts without an explicit route ID remain visible; "
            "they are not confirmed to affect the selected route."
        )
    if alerts_truncated:
        st.warning(f"Showing only the first {ALERT_LIMIT} alerts.")
    if not alert_fresh:
        st.info("Alert feed status is unknown; no current alert count is shown.")
    elif alerts.empty:
        st.info("No active alerts in the latest stored non-empty snapshot for this selection.")
    else:
        display = pd.DataFrame(
            {
                "Severity": alerts["severity"].fillna("Unknown"),
                "Effect": alerts["effect"].fillna("Unknown"),
                "Message": alerts["header_text"].fillna("No heading"),
                "Routes": alerts["route_ids"].apply(_route_scope),
            }
        )
        st.dataframe(display, hide_index=True, width="stretch")
        alert_ids = alerts["alert_id"].astype(str).tolist()
        selected_alert_id = st.selectbox("Alert details", alert_ids)
        selected_alert = alerts.loc[alerts["alert_id"].astype(str) == selected_alert_id].iloc[0]
        st.write(_text(selected_alert["header_text"], "No heading"))
        st.write(_text(selected_alert["description_text"], "No description"))
        if _route_scope(selected_alert["route_ids"]) == "Route not specified":
            st.caption("This alert has no explicit route ID; stop or trip scope is not inferred.")

    st.caption(
        "Alerts come from the latest stored non-empty snapshot. "
        "Newer empty source snapshots are not tracked yet."
    )


show_live_operations()

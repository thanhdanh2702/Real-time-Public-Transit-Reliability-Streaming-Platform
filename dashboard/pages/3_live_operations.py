import os

import pandas as pd
import streamlit as st
from sqlalchemy.exc import SQLAlchemyError

from dashboard.components.vehicle_map import build_vehicle_map, occupancy_label
from dashboard.queries import live_operations, route_reliability

VEHICLE_LIMIT = 2000
ALERT_LIMIT = 100

st.set_page_config(page_title="TransitPulse | Live Operations", page_icon="🗺️", layout="wide")
st.title("Live operations")
st.caption(
    "Latest fresh MBTA bus positions and service alerts · UI refreshes every 30 seconds · "
    "America/New_York"
)


def _local_time(value: object) -> str:
    timezone = os.getenv("APP_TIMEZONE", "America/New_York")
    return pd.to_datetime(value, utc=True).tz_convert(timezone).strftime("%Y-%m-%d %H:%M:%S %Z")


def _route_labels(routes: pd.DataFrame) -> dict[str, str]:
    labels = {}
    for _, row in routes.iterrows():
        route_id = str(row["route_id"])
        short_name = row.get("route_short_name")
        if not isinstance(short_name, str) or not short_name:
            short_name = route_id
        long_name = row.get("route_long_name")
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
    if value is None or (not isinstance(value, (list, tuple)) and pd.isna(value)) or value == "":
        return missing
    return str(value)


def _filter_alerts(alerts: pd.DataFrame, severity: str, effect: str) -> pd.DataFrame:
    result = alerts
    if severity != "All severities":
        result = result[result["severity"].fillna("Unknown") == severity]
    if effect != "All effects":
        result = result[result["effect"].fillna("Unknown") == effect]
    return result


@st.fragment(run_every="30s")
def show_live_operations() -> None:
    try:
        labels = _route_labels(route_reliability.get_routes())
        route_id = st.sidebar.selectbox(
            "Route",
            [None, *labels],
            format_func=lambda value: "All bus routes" if value is None else labels[value],
        )
        directions = {None: "Both directions", 0: "Direction 0", 1: "Direction 1", -1: "Unknown"}
        direction_id = st.sidebar.selectbox(
            "Direction",
            list(directions),
            format_func=lambda value: directions[value],
        )
        vehicle_status = live_operations.get_vehicle_feed_status().iloc[0]
        alert_status = live_operations.get_alert_feed_status().iloc[0]
        vehicle_arguments: dict[str, object] = {"route_id": route_id, "limit": VEHICLE_LIMIT}
        if direction_id is not None:
            vehicle_arguments["direction_id"] = direction_id
        vehicle_rows = live_operations.get_live_vehicles(**vehicle_arguments)
        alert_rows = live_operations.get_active_alerts(route_id=route_id, limit=ALERT_LIMIT)
    except (SQLAlchemyError, pd.errors.DatabaseError, RuntimeError):
        st.error(
            "Could not load live operations from PostgreSQL. Check the database and dbt marts."
        )
        return

    vehicles_truncated = len(vehicle_rows) > VEHICLE_LIMIT
    alerts_truncated = len(alert_rows) > ALERT_LIMIT
    vehicles = vehicle_rows.head(VEHICLE_LIMIT).copy()
    alerts = alert_rows.head(ALERT_LIMIT).copy()
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
    vehicle_metric.metric("Fresh vehicles on map", len(vehicles))
    alert_metric.metric("Active alerts shown", len(alerts) if alert_fresh else "—")

    st.subheader("Bus positions")
    if vehicles_truncated:
        st.warning(f"Showing only the first {VEHICLE_LIMIT} fresh vehicles.")
    if vehicles.empty:
        st.info("No fresh bus positions with usable coordinates match the selected filters.")
    else:
        search = st.text_input("Find vehicle", placeholder="Enter a vehicle ID")
        visible_vehicles = vehicles
        if search.strip():
            visible_vehicles = vehicles[
                vehicles["vehicle_id"].astype(str).str.contains(
                    search.strip(), case=False, regex=False
                )
            ]
        if visible_vehicles.empty:
            st.info("No vehicle ID matches the search text.")
        else:
            vehicle_ids = visible_vehicles["vehicle_id"].astype(str).tolist()
            if st.session_state.get("selected_vehicle_id") not in vehicle_ids:
                st.session_state["selected_vehicle_id"] = vehicle_ids[0]
            selected_id = st.selectbox(
                "Vehicle",
                vehicle_ids,
                key="selected_vehicle_id",
                format_func=lambda value: f"Vehicle {value}",
            )
            selected = visible_vehicles.loc[
                visible_vehicles["vehicle_id"].astype(str) == selected_id
            ].iloc[0]
            deck = build_vehicle_map(visible_vehicles, selected_vehicle_id=selected_id)
            if deck is not None:
                st.pydeck_chart(deck, height=540, width="stretch")

            route_name = _text(selected.get("route_short_name"), _text(selected.get("route_id")))
            detail_columns = st.columns(4)
            detail_columns[0].metric("Vehicle", selected_id)
            detail_columns[1].metric("Route", route_name)
            detail_columns[2].metric("Direction", _text(selected.get("direction_id")))
            detail_columns[3].metric(
                "Event age",
                f"{int(selected['source_age_seconds'])} sec"
                if "source_age_seconds" in selected and pd.notna(selected["source_age_seconds"])
                else "—",
            )
            st.write(f"Trip: {_text(selected.get('trip_id'))}")
            st.write(f"Headsign: {_text(selected.get('trip_headsign'))}")
            st.write(f"Occupancy: {occupancy_label(selected.get('occupancy_status'))}")
            st.caption(f"Vehicle event observed: {_local_time(selected['event_timestamp'])}")

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
        severity_values = sorted(alerts["severity"].fillna("Unknown").unique().tolist())
        effect_values = sorted(alerts["effect"].fillna("Unknown").unique().tolist())
        filter_col, effect_col = st.columns(2)
        severity_filter = filter_col.selectbox(
            "Alert severity", ["All severities", *severity_values]
        )
        effect_filter = effect_col.selectbox("Alert effect", ["All effects", *effect_values])
        visible_alerts = _filter_alerts(alerts, severity_filter, effect_filter)
        if visible_alerts.empty:
            st.info("No alerts match the selected severity and effect.")
        else:
            display = pd.DataFrame(
                {
                    "Severity": visible_alerts["severity"].fillna("Unknown"),
                    "Effect": visible_alerts["effect"].fillna("Unknown"),
                    "Message": visible_alerts["header_text"].fillna("No heading"),
                    "Routes": visible_alerts["route_ids"].apply(_route_scope),
                }
            )
            st.dataframe(display, hide_index=True, width="stretch")
            alert_ids = visible_alerts["alert_id"].astype(str).tolist()
            if st.session_state.get("selected_alert_id") not in alert_ids:
                st.session_state["selected_alert_id"] = alert_ids[0]
            selected_alert_id = st.selectbox(
                "Alert details", alert_ids, key="selected_alert_id"
            )
            selected_alert = visible_alerts.loc[
                visible_alerts["alert_id"].astype(str) == selected_alert_id
            ].iloc[0]
            st.markdown(f"**{_text(selected_alert.get('header_text'), 'No heading')}**")
            st.write(_text(selected_alert.get("description_text"), "No description"))
            st.caption(
                f"Severity: {_text(selected_alert.get('severity'), 'Unknown')} · "
                f"Effect: {_text(selected_alert.get('effect'), 'Unknown')} · "
                f"Cause: {_text(selected_alert.get('cause'), 'Unknown')}"
            )
            if _route_scope(selected_alert.get("route_ids")) == "Route not specified":
                st.caption(
                    "This alert has no explicit route ID; stop or trip scope is not inferred."
                )

    st.caption(
        "Alerts come from the latest stored non-empty snapshot. "
        "Newer empty source snapshots are not tracked yet."
    )


show_live_operations()

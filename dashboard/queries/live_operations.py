import pandas as pd

from dashboard.queries.database import read_dataframe


def _check_limit(limit: int) -> None:
    if not 1 <= limit <= 2000:
        raise ValueError("limit must be between 1 and 2000")


def get_vehicle_feed_status() -> pd.DataFrame:
    """Check bus-position freshness independently of the selected route."""
    return read_dataframe(
        """
        SELECT
            MAX(event_timestamp) AS latest_event_at,
            COUNT(CASE WHEN is_fresh THEN 1 END) AS fresh_vehicle_count
        FROM mart.vehicle_latest_state
        WHERE route_type = 3
        """
    )


def get_live_vehicles(route_id: str | None = None, limit: int = 2000) -> pd.DataFrame:
    """Return one extra vehicle so the page can detect a truncated map."""
    _check_limit(limit)
    route_clause = "AND route_id = :route_id" if route_id is not None else ""
    parameters: dict[str, object] = {"limit": limit + 1}
    if route_id is not None:
        parameters["route_id"] = route_id
    return read_dataframe(
        f"""
        SELECT
            vehicle_id, event_timestamp, route_id, route_short_name,
            trip_id, trip_headsign, direction_id, latitude, longitude,
            occupancy_status
        FROM mart.vehicle_latest_state
        WHERE route_type = 3
            AND is_fresh
            AND latitude BETWEEN -90 AND 90
            AND longitude BETWEEN -180 AND 180
            {route_clause}
        ORDER BY event_timestamp DESC, vehicle_id
        LIMIT :limit
        """,
        parameters,
    )


def get_alert_feed_status() -> pd.DataFrame:
    """A missing row means the latest empty source snapshot is not observable."""
    return read_dataframe(
        """
        SELECT
            MAX(feed_timestamp) AS latest_feed_at,
            COUNT(CASE WHEN is_feed_fresh THEN 1 END) AS fresh_snapshot_entities
        FROM mart.service_alerts_latest_snapshot
        """
    )


def get_active_alerts(route_id: str | None = None, limit: int = 100) -> pd.DataFrame:
    """Keep alerts without a route ID visible, but do not infer their route impact."""
    _check_limit(limit)
    route_clause = (
        "AND (:route_id = ANY(route_ids) OR CARDINALITY(route_ids) = 0)"
        if route_id is not None
        else ""
    )
    parameters: dict[str, object] = {"limit": limit + 1}
    if route_id is not None:
        parameters["route_id"] = route_id
    return read_dataframe(
        f"""
        SELECT
            alert_id, feed_timestamp, severity, effect, header_text,
            description_text, route_ids, stop_ids, trip_ids, direction_ids
        FROM mart.service_alerts_latest_snapshot
        WHERE is_display_active_now
            AND is_feed_fresh
            {route_clause}
        ORDER BY feed_timestamp DESC, alert_id
        LIMIT :limit
        """,
        parameters,
    )

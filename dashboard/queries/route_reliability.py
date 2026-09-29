from datetime import datetime

import pandas as pd

from dashboard.queries.database import read_dataframe


def _where(
    since: datetime | None = None,
    route_id: str | None = None,
    direction_id: int | None = None,
    bucket: object | None = None,
) -> tuple[str, dict[str, object]]:
    conditions = []
    parameters: dict[str, object] = {}
    if since is not None:
        conditions.append("observation_bucket >= :cutoff")
        parameters["cutoff"] = since
    if route_id is not None:
        conditions.append("route_id = :route_id")
        parameters["route_id"] = route_id
    if direction_id is not None:
        conditions.append("direction_id = :direction_id")
        parameters["direction_id"] = direction_id
    if bucket is not None:
        conditions.append("observation_bucket = :bucket")
        parameters["bucket"] = bucket
    return " AND ".join(conditions) or "1 = 1", parameters


def get_routes() -> pd.DataFrame:
    return read_dataframe(
        """
        SELECT route_id, route_short_name, route_long_name
        FROM mart.dim_routes
        WHERE route_type = 3
        ORDER BY route_short_name, route_id
        """
    )


def get_trend(
    since: datetime, route_id: str | None = None, direction_id: int | None = None
) -> pd.DataFrame:
    where, parameters = _where(since, route_id, direction_id)
    return read_dataframe(
        f"""
        SELECT
            observation_bucket,
            SUM(observed_trip_count) AS observed_trip_count,
            SUM(eligible_trip_count) AS eligible_trip_count,
            SUM(late_trip_count) AS late_trip_count,
            CASE WHEN SUM(eligible_trip_count) > 0
                THEN ROUND(100.0 * SUM(late_trip_count) / SUM(eligible_trip_count), 1)
            END AS predicted_late_percentage
        FROM mart.route_health_5m
        WHERE {where}
        GROUP BY observation_bucket
        ORDER BY observation_bucket
        """,
        parameters,
    )


def get_route_comparison(
    since: datetime, route_id: str | None = None, direction_id: int | None = None
) -> pd.DataFrame:
    where, parameters = _where(since, route_id, direction_id)
    return read_dataframe(
        f"""
        SELECT
            route_id,
            MAX(route_short_name) AS route_short_name,
            SUM(observed_trip_count) AS observed_trip_count,
            SUM(eligible_trip_count) AS eligible_trip_count,
            SUM(late_trip_count) AS late_trip_count,
            CASE WHEN SUM(eligible_trip_count) > 0
                THEN ROUND(100.0 * SUM(late_trip_count) / SUM(eligible_trip_count), 1)
            END AS predicted_late_percentage
        FROM mart.route_health_5m
        WHERE {where}
        GROUP BY route_id
        """,
        parameters,
    )


def get_latest_bucket(route_id: str | None = None, direction_id: int | None = None) -> pd.DataFrame:
    where, parameters = _where(route_id=route_id, direction_id=direction_id)
    return read_dataframe(
        f"SELECT MAX(observation_bucket) AS latest_bucket FROM mart.route_health_5m WHERE {where}",
        parameters,
    )


def get_trip_samples(
    since: datetime,
    route_id: str | None = None,
    direction_id: int | None = None,
    bucket: object | None = None,
    limit: int = 100,
) -> pd.DataFrame:
    if not 1 <= limit <= 500:
        raise ValueError("limit must be between 1 and 500")
    where, parameters = _where(since, route_id, direction_id, bucket)
    parameters["limit"] = limit
    return read_dataframe(
        f"""
        SELECT
            observation_bucket,
            observation_local_timestamp,
            route_id,
            route_short_name,
            direction_id,
            trip_id,
            trip_instance_key,
            stop_name,
            scheduled_prediction_timestamp,
            delay_prediction_timestamp,
            predicted_delay_seconds,
            is_predicted_late
        FROM mart.fct_trip_monitoring_samples
        WHERE route_type = 3 AND {where}
        ORDER BY observation_bucket DESC, event_timestamp DESC, trip_instance_key
        LIMIT :limit
        """,
        parameters,
    )

from datetime import datetime

import pandas as pd

from dashboard.queries.database import read_dataframe


def get_vehicle_status() -> pd.DataFrame:
    return read_dataframe(
        """
        SELECT
            COUNT(CASE WHEN is_fresh THEN 1 END) AS fresh_vehicle_count,
            MAX(event_timestamp) AS latest_event_at
        FROM mart.vehicle_latest_state
        WHERE route_type = 3
        """
    )


def get_alert_status() -> pd.DataFrame:
    return read_dataframe(
        """
        SELECT
            COUNT(CASE WHEN is_display_active_now AND is_feed_fresh THEN 1 END)
                AS active_alert_count,
            MAX(feed_timestamp) AS latest_feed_at
        FROM mart.service_alerts_latest_snapshot
        """
    )


def get_latest_health_bucket() -> pd.DataFrame:
    return read_dataframe(
        "SELECT MAX(observation_bucket) AS latest_bucket FROM mart.route_health_5m"
    )


def get_health_trend(since: datetime) -> pd.DataFrame:
    return read_dataframe(
        """
        SELECT
            observation_bucket,
            SUM(observed_trip_count) AS observed_trip_count,
            SUM(eligible_trip_count) AS eligible_trip_count,
            SUM(late_trip_count) AS late_trip_count,
            CASE
                WHEN SUM(eligible_trip_count) > 0
                    THEN ROUND(100.0 * SUM(late_trip_count) / SUM(eligible_trip_count), 1)
            END AS predicted_late_percentage
        FROM mart.route_health_5m
        WHERE observation_bucket >= :cutoff
        GROUP BY observation_bucket
        ORDER BY observation_bucket
        """,
        {"cutoff": since},
    )


def get_routes_needing_attention(
    start_time: datetime,
    end_time: datetime,
    *,
    min_eligible_samples: int = 20,
    limit: int = 10,
) -> pd.DataFrame:
    if min_eligible_samples < 1:
        raise ValueError("min_eligible_samples must be positive")
    if not 1 <= limit <= 50:
        raise ValueError("limit must be between 1 and 50")
    return read_dataframe(
        """
        SELECT
            route_id,
            MAX(route_short_name) AS route_short_name,
            direction_id,
            COUNT(*) AS observed_trip_count,
            COUNT(*) FILTER (WHERE predicted_delay_seconds IS NOT NULL)
                AS eligible_trip_count,
            ROUND(
                100.0 * COUNT(*) FILTER (WHERE is_predicted_late IS TRUE)
                / NULLIF(
                    COUNT(*) FILTER (WHERE predicted_delay_seconds IS NOT NULL),
                    0
                ),
                1
            ) AS predicted_late_percentage,
            ROUND(
                100.0 * COUNT(*) FILTER (WHERE predicted_delay_seconds IS NOT NULL)
                / NULLIF(COUNT(*), 0),
                1
            ) AS metric_coverage_percentage,
            ROUND(
                PERCENTILE_CONT(0.9) WITHIN GROUP (
                    ORDER BY GREATEST(predicted_delay_seconds, 0) / 60.0
                ) FILTER (WHERE predicted_delay_seconds IS NOT NULL)::NUMERIC,
                1
            ) AS p90_predicted_lateness_minutes
        FROM mart.fct_trip_monitoring_samples
        WHERE
            route_type = 3
            AND event_timestamp >= :start_time
            AND event_timestamp < :end_time
            AND route_id IS NOT NULL
        GROUP BY route_id, direction_id
        HAVING COUNT(*) FILTER (WHERE predicted_delay_seconds IS NOT NULL)
            >= :min_eligible_samples
        ORDER BY predicted_late_percentage DESC NULLS LAST,
            p90_predicted_lateness_minutes DESC NULLS LAST,
            eligible_trip_count DESC
        LIMIT :limit
        """,
        {
            "start_time": start_time,
            "end_time": end_time,
            "min_eligible_samples": min_eligible_samples,
            "limit": limit,
        },
    )

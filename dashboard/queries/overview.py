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

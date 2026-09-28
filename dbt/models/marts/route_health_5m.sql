SELECT
    observation_bucket,
    observation_local_timestamp,
    observation_local_date,
    route_id,
    direction_id,
    MAX(route_short_name) AS route_short_name,
    MAX(route_long_name) AS route_long_name,
    MAX(route_type) AS route_type,
    MAX(route_color) AS route_color,
    COUNT(*) AS observed_trip_count,
    COUNT(*) FILTER (
        WHERE delay_basis = 'arrival' AND predicted_delay_seconds IS NOT NULL
    ) AS eligible_trip_count,
    COUNT(*) FILTER (
        WHERE
            delay_basis = 'arrival'
            AND predicted_delay_seconds > {{ var('predicted_late_threshold_seconds') }}
    ) AS late_trip_count,
    COUNT(DISTINCT vehicle_id) AS observed_vehicle_count,
    COUNT(*) FILTER (WHERE has_gtfs_schedule_match) AS gtfs_schedule_match_count,
    ROUND(
        100.0 * COUNT(*) FILTER (
            WHERE delay_basis = 'arrival' AND predicted_delay_seconds IS NOT NULL
        ) / NULLIF(COUNT(*), 0),
        2
    ) AS metric_coverage_percentage,
    ROUND(
        100.0 * COUNT(*) FILTER (
            WHERE
                delay_basis = 'arrival'
                AND predicted_delay_seconds > {{ var('predicted_late_threshold_seconds') }}
        ) / NULLIF(
            COUNT(*) FILTER (
                WHERE delay_basis = 'arrival' AND predicted_delay_seconds IS NOT NULL
            ),
            0
        ),
        2
    ) AS predicted_late_percentage,
    ROUND(
        AVG(GREATEST(predicted_delay_seconds, 0)) FILTER (
            WHERE delay_basis = 'arrival'
        ) / 60.0,
        2
    ) AS average_predicted_lateness_minutes,
    ROUND(
        PERCENTILE_CONT(0.9) WITHIN GROUP (
            ORDER BY GREATEST(predicted_delay_seconds, 0) / 60.0
        ) FILTER (WHERE delay_basis = 'arrival')::NUMERIC,
        2
    ) AS p90_predicted_lateness_minutes
FROM {{ ref('fct_trip_monitoring_samples') }}
WHERE
    route_id IS NOT NULL
    AND route_type = {{ var('dashboard_route_type') }}
GROUP BY
    observation_bucket,
    observation_local_timestamp,
    observation_local_date,
    route_id,
    direction_id

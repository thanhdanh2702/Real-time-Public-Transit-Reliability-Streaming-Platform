WITH expected AS (
    SELECT
        observation_bucket,
        route_id,
        direction_id,
        COUNT(*) FILTER (
            WHERE predicted_delay_seconds IS NOT NULL
        ) AS eligible_trip_count,
        COUNT(*) FILTER (
            WHERE predicted_delay_seconds > {{ var('predicted_late_threshold_seconds') }}
        ) AS late_trip_count
    FROM {{ ref('fct_trip_monitoring_samples') }}
    WHERE
        route_id IS NOT NULL
        AND route_type = {{ var('dashboard_route_type') }}
    GROUP BY observation_bucket, route_id, direction_id
)

SELECT
    actual.observation_bucket,
    actual.route_id,
    actual.direction_id,
    expected.eligible_trip_count AS expected_eligible_trip_count,
    actual.eligible_trip_count AS actual_eligible_trip_count,
    expected.late_trip_count AS expected_late_trip_count,
    actual.late_trip_count AS actual_late_trip_count
FROM {{ ref('route_health_5m') }} AS actual
INNER JOIN expected
    USING (observation_bucket, route_id, direction_id)
WHERE
    actual.eligible_trip_count <> expected.eligible_trip_count
    OR actual.late_trip_count <> expected.late_trip_count

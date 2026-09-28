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
        ) AS late_trip_count,
        ROUND(
            100.0 * COUNT(*) FILTER (WHERE predicted_delay_seconds IS NOT NULL)
                / NULLIF(COUNT(*), 0),
            2
        ) AS metric_coverage_percentage,
        ROUND(
            100.0 * COUNT(*) FILTER (
                WHERE predicted_delay_seconds > {{ var('predicted_late_threshold_seconds') }}
            ) / NULLIF(COUNT(*) FILTER (WHERE predicted_delay_seconds IS NOT NULL), 0),
            2
        ) AS predicted_late_percentage,
        ROUND(
            AVG(GREATEST(predicted_delay_seconds, 0))
                FILTER (WHERE predicted_delay_seconds IS NOT NULL) / 60.0,
            2
        ) AS average_predicted_lateness_minutes,
        ROUND(
            PERCENTILE_CONT(0.9) WITHIN GROUP (
                ORDER BY GREATEST(predicted_delay_seconds, 0) / 60.0
            ) FILTER (WHERE predicted_delay_seconds IS NOT NULL)::NUMERIC,
            2
        ) AS p90_predicted_lateness_minutes
    FROM {{ ref('fct_trip_monitoring_samples') }}
    WHERE
        route_id IS NOT NULL
        AND route_type = {{ var('dashboard_route_type') }}
    GROUP BY observation_bucket, route_id, direction_id
)

SELECT
    COALESCE(actual.observation_bucket, expected.observation_bucket) AS observation_bucket,
    COALESCE(actual.route_id, expected.route_id) AS route_id,
    COALESCE(actual.direction_id, expected.direction_id) AS direction_id,
    expected.eligible_trip_count AS expected_eligible_trip_count,
    actual.eligible_trip_count AS actual_eligible_trip_count,
    expected.late_trip_count AS expected_late_trip_count,
    actual.late_trip_count AS actual_late_trip_count,
    expected.metric_coverage_percentage AS expected_metric_coverage_percentage,
    actual.metric_coverage_percentage AS actual_metric_coverage_percentage,
    expected.predicted_late_percentage AS expected_predicted_late_percentage,
    actual.predicted_late_percentage AS actual_predicted_late_percentage,
    expected.average_predicted_lateness_minutes AS expected_average_lateness,
    actual.average_predicted_lateness_minutes AS actual_average_lateness,
    expected.p90_predicted_lateness_minutes AS expected_p90_lateness,
    actual.p90_predicted_lateness_minutes AS actual_p90_lateness
FROM {{ ref('route_health_5m') }} AS actual
FULL OUTER JOIN expected
    USING (observation_bucket, route_id, direction_id)
WHERE
    actual.eligible_trip_count IS DISTINCT FROM expected.eligible_trip_count
    OR actual.late_trip_count IS DISTINCT FROM expected.late_trip_count
    OR actual.metric_coverage_percentage IS DISTINCT FROM expected.metric_coverage_percentage
    OR actual.predicted_late_percentage IS DISTINCT FROM expected.predicted_late_percentage
    OR actual.average_predicted_lateness_minutes
        IS DISTINCT FROM expected.average_predicted_lateness_minutes
    OR actual.p90_predicted_lateness_minutes
        IS DISTINCT FROM expected.p90_predicted_lateness_minutes

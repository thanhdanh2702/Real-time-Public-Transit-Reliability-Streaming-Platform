SELECT *
FROM {{ ref('route_health_5m') }}
WHERE
    eligible_trip_count > observed_trip_count
    OR late_trip_count > eligible_trip_count
    OR predicted_late_percentage < 0
    OR predicted_late_percentage > 100
    OR (eligible_trip_count = 0 AND predicted_late_percentage IS NOT NULL)
    OR (eligible_trip_count > 0 AND predicted_late_percentage IS NULL)
    OR p90_predicted_lateness_minutes < 0

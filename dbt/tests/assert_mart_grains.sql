WITH duplicate_grains AS (
    SELECT
        'fct_trip_monitoring_samples' AS model_name,
        observation_bucket::TEXT || ':' || trip_instance_key AS grain_key
    FROM {{ ref('fct_trip_monitoring_samples') }}
    GROUP BY observation_bucket, trip_instance_key
    HAVING COUNT(*) > 1

    UNION ALL

    SELECT
        'route_health_5m' AS model_name,
        observation_bucket::TEXT || ':' || route_id || ':' || direction_id::TEXT
    FROM {{ ref('route_health_5m') }}
    GROUP BY observation_bucket, route_id, direction_id
    HAVING COUNT(*) > 1
)

SELECT *
FROM duplicate_grains

SELECT *
FROM {{ ref('route_health_5m') }}
WHERE route_type <> {{ var('dashboard_route_type') }}

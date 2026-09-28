WITH ranked_positions AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY vehicle_id
            ORDER BY event_timestamp DESC, ingested_at DESC, event_id DESC
        ) AS position_rank
    FROM {{ ref('int_vehicle_positions_enriched') }}
)

SELECT
    event_id,
    vehicle_id,
    event_timestamp,
    event_timestamp AT TIME ZONE '{{ var('project_timezone') }}' AS event_local_timestamp,
    feed_timestamp,
    ingested_at,
    published_at,
    resolved_route_id AS route_id,
    route_short_name,
    route_long_name,
    route_type,
    route_color,
    trip_id,
    service_id,
    trip_headsign,
    gtfs_direction_id AS direction_id,
    latitude,
    longitude,
    bearing,
    speed_mps,
    odometer,
    occupancy_status,
    has_gtfs_trip_match,
    has_gtfs_route_match,
    GREATEST(EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - event_timestamp)), 0)::BIGINT
        AS source_age_seconds,
    event_timestamp >= CURRENT_TIMESTAMP
        - INTERVAL '{{ var('realtime_freshness_seconds') }} seconds' AS is_fresh,
    kafka_topic,
    kafka_partition,
    kafka_offset
FROM ranked_positions
WHERE position_rank = 1

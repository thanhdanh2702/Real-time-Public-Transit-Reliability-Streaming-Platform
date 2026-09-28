WITH vehicle_positions AS (
    SELECT *
    FROM {{ ref('vehicle_positions') }}
),

trips AS (
    SELECT *
    FROM {{ ref('gtfs_trips') }}
),

routes AS (
    SELECT *
    FROM {{ ref('gtfs_routes') }}
)

SELECT
    v.event_id,
    v.vehicle_id,
    v.event_timestamp,
    v.feed_timestamp,
    v.published_at,
    v.ingested_at,
    v.route_id,
    v.trip_id,
    v.latitude,
    v.longitude,
    v.bearing,
    v.odometer,
    v.speed_mps,
    v.occupancy_status,
    v.source,
    v.schema_version,
    v.kafka_topic,
    v.kafka_partition,
    v.kafka_offset,
    v.kafka_timestamp,
    v.created_at,
    COALESCE(v.route_id, t.route_id) AS resolved_route_id,
    t.route_id AS gtfs_trip_route_id,
    t.service_id,
    t.trip_headsign,
    t.direction_id AS gtfs_direction_id,
    r.route_short_name,
    r.route_long_name,
    r.route_type,
    r.route_color,
    t.trip_id IS NOT NULL AS has_gtfs_trip_match,
    r.route_id IS NOT NULL AS has_gtfs_route_match
FROM vehicle_positions AS v

LEFT JOIN trips AS t
    ON v.trip_id = t.trip_id

LEFT JOIN routes AS r
    ON COALESCE(v.route_id, t.route_id) = r.route_id

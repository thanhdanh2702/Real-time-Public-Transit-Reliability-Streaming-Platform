WITH trip_stop_updates AS (
    SELECT *
    FROM {{ ref('trip_stop_updates') }}
),

schedule AS (
    SELECT *
    FROM {{ ref('int_gtfs_trip_stop_schedule') }}
),

routes AS (
    SELECT *
    FROM {{ ref('gtfs_routes') }}
),

stops AS (
    SELECT *
    FROM {{ ref('gtfs_stops') }}
)

SELECT
    u.event_id,
    u.stop_update_index,
    u.event_timestamp,
    u.feed_timestamp,
    u.ingested_at,
    u.published_at,
    u.trip_id,
    u.start_date,
    u.start_time,
    u.route_id,
    u.vehicle_id,
    u.direction_id,
    u.trip_schedule_relationship,
    u.trip_delay_seconds,
    u.stop_id,
    u.stop_sequence,
    u.predicted_arrival,
    u.predicted_departure,
    u.delay_seconds,
    u.stop_schedule_relationship,
    u.source,
    u.schema_version,
    u.kafka_topic,
    u.kafka_partition,
    u.kafka_offset,
    u.kafka_timestamp,
    u.created_at,
    COALESCE(u.route_id, sc.route_id) AS resolved_route_id,
    sc.stop_id AS scheduled_stop_id,
    sc.scheduled_arrival_time,
    sc.scheduled_departure_time,
    sc.service_id,
    sc.trip_headsign,
    sc.direction_id AS scheduled_direction_id,
    COALESCE(sc.route_short_name, r.route_short_name) AS route_short_name,
    COALESCE(sc.route_long_name, r.route_long_name) AS route_long_name,
    COALESCE(sc.route_type, r.route_type) AS route_type,
    COALESCE(sc.route_color, r.route_color) AS route_color,
    COALESCE(sc.stop_name, s.stop_name) AS stop_name,
    COALESCE(sc.stop_lat, s.stop_lat) AS stop_lat,
    COALESCE(sc.stop_lon, s.stop_lon) AS stop_lon,
    COALESCE(sc.parent_station, s.parent_station) AS parent_station,
    COALESCE(sc.platform_code, s.platform_code) AS platform_code,
    sc.trip_id IS NOT NULL AS has_gtfs_schedule_match,
    s.stop_id IS NOT NULL AS has_gtfs_stop_match,
    r.route_id IS NOT NULL OR sc.route_id IS NOT NULL AS has_gtfs_route_match
FROM trip_stop_updates AS u

LEFT JOIN schedule AS sc
    ON u.trip_id = sc.trip_id
    AND u.stop_sequence = sc.stop_sequence

LEFT JOIN routes AS r
    ON COALESCE(u.route_id, sc.route_id) = r.route_id

LEFT JOIN stops AS s
    ON u.stop_id = s.stop_id

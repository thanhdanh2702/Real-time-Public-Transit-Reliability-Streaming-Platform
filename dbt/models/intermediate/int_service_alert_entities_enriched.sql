WITH service_alert_entities AS (
    SELECT *
    FROM {{ ref('service_alert_entities') }}
),

trips AS (
    SELECT *
    FROM {{ ref('gtfs_trips') }}
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
    a.event_id,
    a.informed_entity_index,
    a.alert_id,
    a.event_timestamp,
    a.feed_timestamp,
    a.ingested_at,
    a.published_at,
    a.cause,
    a.effect,
    a.severity,
    a.header_text,
    a.description_text,
    a.url,
    a.active_periods,
    a.agency_id,
    a.route_id,
    a.route_type,
    a.trip_id,
    a.stop_id,
    a.direction_id,
    a.source,
    a.schema_version,
    a.kafka_topic,
    a.kafka_partition,
    a.kafka_offset,
    a.kafka_timestamp,
    a.created_at,
    COALESCE(a.route_id, t.route_id) AS resolved_route_id,
    t.service_id,
    t.trip_headsign,
    r.route_short_name,
    r.route_long_name,
    r.route_color,
    s.stop_name,
    s.stop_lat,
    s.stop_lon,
    s.parent_station,
    s.platform_code,
    t.trip_id IS NOT NULL AS has_gtfs_trip_match,
    r.route_id IS NOT NULL AS has_gtfs_route_match,
    s.stop_id IS NOT NULL AS has_gtfs_stop_match
FROM service_alert_entities AS a

LEFT JOIN trips AS t
    ON a.trip_id = t.trip_id

LEFT JOIN routes AS r
    ON COALESCE(a.route_id, t.route_id) = r.route_id

LEFT JOIN stops AS s
    ON a.stop_id = s.stop_id

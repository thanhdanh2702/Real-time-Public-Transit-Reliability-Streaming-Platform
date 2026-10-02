WITH alerts AS (
    SELECT *
    FROM {{ ref('int_service_alert_entities_enriched') }}
)

SELECT
    a.event_id,
    a.informed_entity_index,
    a.alert_id,
    period.active_period_index,
    (period.value ->> 'start')::TIMESTAMPTZ AS active_period_start,
    (period.value ->> 'end')::TIMESTAMPTZ AS active_period_end,
    a.event_timestamp,
    a.feed_timestamp,
    a.cause,
    a.effect,
    a.severity,
    a.header_text,
    a.description_text,
    a.url,
    a.agency_id,
    a.route_id,
    a.resolved_route_id,
    a.route_type,
    a.route_short_name,
    a.route_long_name,
    a.route_color,
    a.trip_id,
    a.trip_headsign,
    a.stop_id,
    a.stop_name,
    a.stop_lat,
    a.stop_lon,
    a.direction_id,
    a.has_gtfs_trip_match,
    a.has_gtfs_route_match,
    a.has_gtfs_stop_match,
    a.kafka_topic,
    a.kafka_partition,
    a.kafka_offset,
    a.kafka_timestamp
FROM alerts AS a

LEFT JOIN LATERAL JSONB_ARRAY_ELEMENTS(a.active_periods)
    WITH ORDINALITY AS period(value, active_period_index)
    ON TRUE

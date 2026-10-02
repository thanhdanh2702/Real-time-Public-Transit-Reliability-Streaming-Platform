WITH stop_times AS (
    SELECT *
    FROM {{ ref('gtfs_stop_times') }}
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
    st.trip_id,
    st.stop_sequence,
    st.stop_id,
    st.arrival_time AS scheduled_arrival_time,
    st.departure_time AS scheduled_departure_time,
    st.stop_headsign,
    st.pickup_type,
    st.drop_off_type,
    st.timepoint,
    t.route_id,
    t.service_id,
    t.trip_headsign,
    t.trip_short_name,
    t.direction_id,
    t.block_id,
    t.shape_id,
    t.wheelchair_accessible,
    t.bikes_allowed,
    r.agency_id,
    r.route_short_name,
    r.route_long_name,
    r.route_type,
    r.route_color,
    r.route_text_color,
    s.stop_code,
    s.stop_name,
    s.stop_lat,
    s.stop_lon,
    s.location_type,
    s.parent_station,
    s.wheelchair_boarding,
    s.platform_code
FROM stop_times AS st

LEFT JOIN trips AS t
    ON st.trip_id = t.trip_id

LEFT JOIN routes AS r
    ON t.route_id = r.route_id

LEFT JOIN stops AS s
    ON st.stop_id = s.stop_id

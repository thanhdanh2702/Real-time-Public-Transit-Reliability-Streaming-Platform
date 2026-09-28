SELECT
    route_id,
    agency_id,
    route_short_name,
    route_long_name,
    route_desc,
    route_type,
    route_color,
    route_text_color,
    route_url,
    route_sort_order,
    route_fare_class,
    line_id,
    listed_route,
    network_id
FROM {{ ref('gtfs_routes') }}

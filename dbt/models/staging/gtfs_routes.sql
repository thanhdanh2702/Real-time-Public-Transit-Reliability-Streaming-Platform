SELECT
    *
FROM
    {{ source('gtfs_static', 'gtfs_routes') }}

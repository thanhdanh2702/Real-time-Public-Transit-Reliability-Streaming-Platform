SELECT
    *
FROM
    {{ source('gtfs_static', 'gtfs_stop_times') }}

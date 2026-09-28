SELECT
    *
FROM
    {{ source('realtime', 'trip_stop_updates') }}
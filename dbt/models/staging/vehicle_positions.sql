SELECT
    *
FROM
    {{ source('realtime', 'vehicle_positions') }}
SELECT
    *
FROM
    {{ source('realtime', 'service_alert_entities') }}

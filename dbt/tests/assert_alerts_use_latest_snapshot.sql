WITH latest_source_snapshot AS (
    SELECT MAX(feed_timestamp) AS feed_timestamp
    FROM {{ ref('service_alert_entities') }}
)

SELECT alerts.*
FROM {{ ref('service_alerts_latest_snapshot') }} AS alerts
CROSS JOIN latest_source_snapshot AS source_snapshot
WHERE alerts.feed_timestamp IS DISTINCT FROM source_snapshot.feed_timestamp

WITH latest_snapshot AS (
    SELECT MAX(feed_timestamp) AS feed_timestamp
    FROM {{ ref('int_service_alert_entities_enriched') }}
),

snapshot_entities AS (
    SELECT alerts.*
    FROM {{ ref('int_service_alert_entities_enriched') }} AS alerts
    INNER JOIN latest_snapshot
        ON alerts.feed_timestamp = latest_snapshot.feed_timestamp
),

entity_flags AS (
    SELECT
        *,
        CASE
            WHEN active_periods IS NULL OR JSONB_ARRAY_LENGTH(active_periods) = 0
                THEN TRUE
            ELSE EXISTS (
                SELECT 1
                FROM JSONB_ARRAY_ELEMENTS(active_periods) AS period(value)
                WHERE
                    (
                        period.value ->> 'start' IS NULL
                        OR (period.value ->> 'start')::TIMESTAMPTZ <= CURRENT_TIMESTAMP
                    )
                    AND (
                        period.value ->> 'end' IS NULL
                        OR (period.value ->> 'end')::TIMESTAMPTZ >= CURRENT_TIMESTAMP
                    )
            )
        END AS is_display_active_now
    FROM snapshot_entities
)

SELECT
    alert_id,
    MAX(event_timestamp) AS event_timestamp,
    MAX(feed_timestamp) AS feed_timestamp,
    MAX(ingested_at) AS ingested_at,
    MAX(cause) AS cause,
    MAX(effect) AS effect,
    MAX(severity) AS severity,
    MAX(header_text) AS header_text,
    MAX(description_text) AS description_text,
    MAX(url) AS url,
    COUNT(*) AS informed_entity_count,
    COALESCE(
        ARRAY_AGG(DISTINCT resolved_route_id) FILTER (WHERE resolved_route_id IS NOT NULL),
        ARRAY[]::TEXT[]
    ) AS route_ids,
    COALESCE(
        ARRAY_AGG(DISTINCT stop_id) FILTER (WHERE stop_id IS NOT NULL),
        ARRAY[]::TEXT[]
    ) AS stop_ids,
    COALESCE(
        ARRAY_AGG(DISTINCT trip_id) FILTER (WHERE trip_id IS NOT NULL),
        ARRAY[]::TEXT[]
    ) AS trip_ids,
    COALESCE(
        ARRAY_AGG(DISTINCT direction_id) FILTER (WHERE direction_id IS NOT NULL),
        ARRAY[]::INTEGER[]
    ) AS direction_ids,
    BOOL_OR(is_display_active_now) AS is_display_active_now,
    GREATEST(EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX(feed_timestamp))), 0)::BIGINT
        AS source_age_seconds,
    MAX(feed_timestamp) >= CURRENT_TIMESTAMP
        - INTERVAL '{{ var('alert_freshness_seconds') }} seconds' AS is_feed_fresh
FROM entity_flags
GROUP BY alert_id

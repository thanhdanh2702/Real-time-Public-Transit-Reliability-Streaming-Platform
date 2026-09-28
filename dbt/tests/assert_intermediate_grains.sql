WITH duplicate_grains AS (
    SELECT
        'int_gtfs_trip_stop_schedule' AS model_name,
        trip_id || ':' || stop_sequence::text AS grain_key
    FROM {{ ref('int_gtfs_trip_stop_schedule') }}
    GROUP BY trip_id, stop_sequence
    HAVING COUNT(*) > 1

    UNION ALL

    SELECT
        'int_vehicle_positions_enriched' AS model_name,
        event_id AS grain_key
    FROM {{ ref('int_vehicle_positions_enriched') }}
    GROUP BY event_id
    HAVING COUNT(*) > 1

    UNION ALL

    SELECT
        'int_trip_stop_updates_enriched' AS model_name,
        event_id || ':' || stop_update_index::text AS grain_key
    FROM {{ ref('int_trip_stop_updates_enriched') }}
    GROUP BY event_id, stop_update_index
    HAVING COUNT(*) > 1

    UNION ALL

    SELECT
        'int_service_alert_entities_enriched' AS model_name,
        event_id || ':' || informed_entity_index::text AS grain_key
    FROM {{ ref('int_service_alert_entities_enriched') }}
    GROUP BY event_id, informed_entity_index
    HAVING COUNT(*) > 1

    UNION ALL

    SELECT
        'int_service_alert_periods' AS model_name,
        event_id || ':' || informed_entity_index::text || ':'
            || COALESCE(active_period_index::text, 'none') AS grain_key
    FROM {{ ref('int_service_alert_periods') }}
    GROUP BY event_id, informed_entity_index, active_period_index
    HAVING COUNT(*) > 1
)

SELECT *
FROM duplicate_grains

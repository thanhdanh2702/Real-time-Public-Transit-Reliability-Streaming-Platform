WITH row_counts AS (
    SELECT
        'int_gtfs_trip_stop_schedule' AS model_name,
        (SELECT COUNT(*) FROM {{ ref('gtfs_stop_times') }}) AS expected_rows,
        (SELECT COUNT(*) FROM {{ ref('int_gtfs_trip_stop_schedule') }}) AS actual_rows

    UNION ALL

    SELECT
        'int_vehicle_positions_enriched',
        (SELECT COUNT(*) FROM {{ ref('vehicle_positions') }}),
        (SELECT COUNT(*) FROM {{ ref('int_vehicle_positions_enriched') }})

    UNION ALL

    SELECT
        'int_trip_stop_updates_enriched',
        (SELECT COUNT(*) FROM {{ ref('trip_stop_updates') }}),
        (SELECT COUNT(*) FROM {{ ref('int_trip_stop_updates_enriched') }})

    UNION ALL

    SELECT
        'int_service_alert_entities_enriched',
        (SELECT COUNT(*) FROM {{ ref('service_alert_entities') }}),
        (SELECT COUNT(*) FROM {{ ref('int_service_alert_entities_enriched') }})
)

SELECT *
FROM row_counts
WHERE expected_rows <> actual_rows

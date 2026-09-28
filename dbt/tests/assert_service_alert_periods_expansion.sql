WITH expected AS (
    SELECT
        COALESCE(SUM(GREATEST(JSONB_ARRAY_LENGTH(active_periods), 1)), 0) AS row_count
    FROM {{ ref('service_alert_entities') }}
),

actual AS (
    SELECT COUNT(*) AS row_count
    FROM {{ ref('int_service_alert_periods') }}
)

SELECT
    expected.row_count AS expected_rows,
    actual.row_count AS actual_rows
FROM expected
CROSS JOIN actual
WHERE expected.row_count <> actual.row_count

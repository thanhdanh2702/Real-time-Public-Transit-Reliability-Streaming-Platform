WITH date_bounds AS (
    SELECT
        MIN(start_date) AS first_date,
        MAX(end_date) AS last_date
    FROM {{ ref('gtfs_calendar') }}

    UNION ALL

    SELECT
        MIN((event_timestamp AT TIME ZONE '{{ var('project_timezone') }}')::DATE),
        MAX((event_timestamp AT TIME ZONE '{{ var('project_timezone') }}')::DATE)
    FROM {{ ref('vehicle_positions') }}

    UNION ALL

    SELECT
        MIN((event_timestamp AT TIME ZONE '{{ var('project_timezone') }}')::DATE),
        MAX((event_timestamp AT TIME ZONE '{{ var('project_timezone') }}')::DATE)
    FROM {{ ref('trip_stop_updates') }}

    UNION ALL

    SELECT
        MIN((event_timestamp AT TIME ZONE '{{ var('project_timezone') }}')::DATE),
        MAX((event_timestamp AT TIME ZONE '{{ var('project_timezone') }}')::DATE)
    FROM {{ ref('service_alert_entities') }}
),

calendar_bounds AS (
    SELECT
        MIN(first_date) AS first_date,
        MAX(last_date) AS last_date
    FROM date_bounds
),

dates AS (
    SELECT GENERATE_SERIES(
        first_date::TIMESTAMP,
        last_date::TIMESTAMP,
        INTERVAL '1 day'
    )::DATE AS date_day
    FROM calendar_bounds
)

SELECT
    date_day,
    TO_CHAR(date_day, 'YYYYMMDD')::INTEGER AS date_key,
    EXTRACT(YEAR FROM date_day)::INTEGER AS year_number,
    EXTRACT(QUARTER FROM date_day)::INTEGER AS quarter_number,
    EXTRACT(MONTH FROM date_day)::INTEGER AS month_number,
    TO_CHAR(date_day, 'FMMonth') AS month_name,
    EXTRACT(WEEK FROM date_day)::INTEGER AS week_number,
    EXTRACT(ISODOW FROM date_day)::INTEGER AS iso_day_of_week,
    TO_CHAR(date_day, 'FMDay') AS day_name,
    EXTRACT(ISODOW FROM date_day) IN (6, 7) AS is_weekend
FROM dates

WITH stop_predictions AS (
    SELECT
        *,
        DATE_BIN(
            INTERVAL '{{ var('monitoring_bucket_minutes') }} minutes',
            event_timestamp,
            TIMESTAMPTZ '2000-01-01 00:00:00+00'
        ) AS observation_bucket,
        CONCAT_WS(
            '|',
            trip_id,
            COALESCE(TO_CHAR(start_date, 'YYYYMMDD'), 'unknown-date'),
            COALESCE(start_time, 'scheduled')
        ) AS trip_instance_key,
        COALESCE(predicted_arrival, predicted_departure) AS next_prediction_timestamp
    FROM {{ ref('int_trip_stop_updates_enriched') }}
    WHERE COALESCE(predicted_arrival, predicted_departure) >= event_timestamp
),

ranked_events AS (
    SELECT
        *,
        DENSE_RANK() OVER (
            PARTITION BY observation_bucket, trip_instance_key
            ORDER BY event_timestamp DESC, event_id DESC
        ) AS event_rank
    FROM stop_predictions
),

ranked_stops AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY observation_bucket, trip_instance_key
            ORDER BY next_prediction_timestamp, stop_sequence, stop_update_index
        ) AS stop_rank
    FROM ranked_events
    WHERE event_rank = 1
),

next_stops AS (
    SELECT
        *,
        CASE
            WHEN predicted_arrival IS NOT NULL AND scheduled_arrival_time IS NOT NULL
                THEN 'arrival'
            WHEN predicted_departure IS NOT NULL AND scheduled_departure_time IS NOT NULL
                THEN 'departure'
        END AS delay_basis,
        CASE
            WHEN predicted_arrival IS NOT NULL AND scheduled_arrival_time IS NOT NULL
                THEN predicted_arrival
            WHEN predicted_departure IS NOT NULL AND scheduled_departure_time IS NOT NULL
                THEN predicted_departure
        END AS delay_prediction_timestamp,
        CASE
            WHEN predicted_arrival IS NOT NULL AND scheduled_arrival_time IS NOT NULL
                THEN scheduled_arrival_time
            WHEN predicted_departure IS NOT NULL AND scheduled_departure_time IS NOT NULL
                THEN scheduled_departure_time
        END AS scheduled_time_text
    FROM ranked_stops
    WHERE stop_rank = 1
),

schedule_parts AS (
    SELECT
        *,
        CASE
            WHEN scheduled_time_text IS NOT NULL THEN
                SPLIT_PART(scheduled_time_text, ':', 1)::INTEGER * 3600
                + SPLIT_PART(scheduled_time_text, ':', 2)::INTEGER * 60
                + SPLIT_PART(scheduled_time_text, ':', 3)::INTEGER
        END AS scheduled_second_of_service_day
    FROM next_stops
),

schedule_candidates AS (
    SELECT
        *,
        DATE_TRUNC(
            'day',
            delay_prediction_timestamp AT TIME ZONE '{{ var('project_timezone') }}'
        )
        + MOD(scheduled_second_of_service_day, 86400) * INTERVAL '1 second'
            AS scheduled_local_candidate
    FROM schedule_parts
),

calculated_delays AS (
    SELECT
        *,
        CASE
            WHEN delay_basis IS NOT NULL THEN (
                scheduled_local_candidate
                + ROUND(
                    EXTRACT(
                        EPOCH FROM (
                            delay_prediction_timestamp
                                AT TIME ZONE '{{ var('project_timezone') }}'
                            - scheduled_local_candidate
                        )
                    ) / 86400
                ) * INTERVAL '1 day'
            ) AT TIME ZONE '{{ var('project_timezone') }}'
        END AS scheduled_prediction_timestamp
    FROM schedule_candidates
),

final AS (
    SELECT
        *,
        EXTRACT(
            EPOCH FROM (delay_prediction_timestamp - scheduled_prediction_timestamp)
        )::INTEGER AS predicted_delay_seconds
    FROM calculated_delays
)

SELECT
    observation_bucket,
    observation_bucket AT TIME ZONE '{{ var('project_timezone') }}' AS observation_local_timestamp,
    (observation_bucket AT TIME ZONE '{{ var('project_timezone') }}')::DATE
        AS observation_local_date,
    event_id,
    event_timestamp,
    feed_timestamp,
    ingested_at,
    trip_id,
    start_date,
    start_time,
    trip_instance_key,
    resolved_route_id AS route_id,
    COALESCE(direction_id, scheduled_direction_id, -1) AS direction_id,
    vehicle_id,
    stop_id,
    stop_sequence,
    stop_name,
    stop_lat,
    stop_lon,
    route_short_name,
    route_long_name,
    route_type,
    route_color,
    service_id,
    trip_headsign,
    predicted_arrival,
    predicted_departure,
    next_prediction_timestamp,
    scheduled_arrival_time,
    scheduled_departure_time,
    scheduled_prediction_timestamp,
    delay_basis,
    delay_seconds AS raw_feed_delay_seconds,
    predicted_delay_seconds,
    CASE
        WHEN predicted_delay_seconds IS NOT NULL
            THEN GREATEST(predicted_delay_seconds, 0)
    END AS predicted_lateness_seconds,
    predicted_delay_seconds > {{ var('predicted_late_threshold_seconds') }} AS is_predicted_late,
    has_gtfs_schedule_match,
    has_gtfs_route_match,
    has_gtfs_stop_match,
    trip_schedule_relationship,
    stop_schedule_relationship,
    kafka_topic,
    kafka_partition,
    kafka_offset
FROM final

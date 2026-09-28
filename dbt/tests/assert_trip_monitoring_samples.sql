SELECT *
FROM {{ ref('fct_trip_monitoring_samples') }}
WHERE
    next_prediction_timestamp IS NULL
    OR next_prediction_timestamp < event_timestamp
    OR (
        delay_basis IS NOT NULL
        AND (
            predicted_delay_seconds IS NULL
            OR predicted_lateness_seconds IS NULL
            OR predicted_lateness_seconds < 0
        )
    )
    OR (
        delay_basis IS NULL
        AND (
            predicted_delay_seconds IS NOT NULL
            OR predicted_lateness_seconds IS NOT NULL
            OR is_predicted_late IS NOT NULL
        )
    )

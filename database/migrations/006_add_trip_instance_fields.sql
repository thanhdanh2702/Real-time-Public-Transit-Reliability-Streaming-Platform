BEGIN;

ALTER TABLE staging.trip_stop_updates
    ADD COLUMN IF NOT EXISTS start_date DATE,
    ADD COLUMN IF NOT EXISTS start_time TEXT;

ALTER TABLE staging.trip_stop_updates
    DROP CONSTRAINT IF EXISTS trip_stop_updates_start_time_check;

ALTER TABLE staging.trip_stop_updates
    ADD CONSTRAINT trip_stop_updates_start_time_check
    CHECK (
        start_time IS NULL
        OR start_time ~ '^[0-9]{2,}:[0-5][0-9]:[0-5][0-9]$'
    );

COMMIT;

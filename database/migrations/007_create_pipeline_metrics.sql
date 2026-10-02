BEGIN;

CREATE TABLE IF NOT EXISTS ops.spark_application_status (
    application_name TEXT PRIMARY KEY,
    query_id TEXT,
    run_id TEXT,
    status TEXT NOT NULL,
    last_progress_at TIMESTAMPTZ,
    last_batch_id BIGINT,
    error_message TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    terminated_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT spark_application_status_check
        CHECK (status IN ('running', 'idle', 'terminated', 'failed'))
);

CREATE TABLE IF NOT EXISTS ops.spark_streaming_progress (
    progress_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    application_name TEXT NOT NULL,
    query_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    progress_timestamp TIMESTAMPTZ NOT NULL,
    batch_id BIGINT NOT NULL,
    num_input_rows BIGINT NOT NULL,
    input_rows_per_second DOUBLE PRECISION NOT NULL,
    processed_rows_per_second DOUBLE PRECISION NOT NULL,
    batch_duration_ms BIGINT NOT NULL,
    source_offsets JSONB NOT NULL DEFAULT '{}'::JSONB,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT spark_streaming_progress_unique UNIQUE (query_id, run_id, batch_id)
);

CREATE INDEX IF NOT EXISTS spark_progress_application_time_idx
    ON ops.spark_streaming_progress (application_name, progress_timestamp DESC);

CREATE TABLE IF NOT EXISTS ops.dbt_run_history (
    run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dag_run_id TEXT UNIQUE,
    status TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    duration_seconds DOUBLE PRECISION,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT dbt_run_history_status_check
        CHECK (status IN ('running', 'success', 'failed'))
);

COMMIT;

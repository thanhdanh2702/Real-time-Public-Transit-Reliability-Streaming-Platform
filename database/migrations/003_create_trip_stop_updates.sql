BEGIN;

CREATE TABLE staging.trip_stop_updates (
    event_id                       TEXT        NOT NULL,
    stop_update_index              INTEGER     NOT NULL,

    event_timestamp                TIMESTAMPTZ NOT NULL,
    feed_timestamp                 TIMESTAMPTZ NOT NULL,
    ingested_at                    TIMESTAMPTZ NOT NULL,
    published_at                   TIMESTAMPTZ NOT NULL,

    trip_id                        TEXT        NOT NULL,
    route_id                       TEXT,
    vehicle_id                     TEXT,
    direction_id                   INTEGER,
    trip_schedule_relationship     TEXT,
    trip_delay_seconds             INTEGER,

    stop_id                        TEXT,
    stop_sequence                  INTEGER,
    predicted_arrival              TIMESTAMPTZ,
    predicted_departure            TIMESTAMPTZ,
    delay_seconds                  INTEGER,
    stop_schedule_relationship     TEXT,

    source                         TEXT        NOT NULL,
    schema_version                 SMALLINT    NOT NULL,
    kafka_topic                    TEXT        NOT NULL,
    kafka_partition                INTEGER     NOT NULL,
    kafka_offset                   BIGINT      NOT NULL,
    kafka_timestamp                TIMESTAMPTZ NOT NULL,
    created_at                     TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT trip_stop_updates_pk
        PRIMARY KEY (event_id, stop_update_index),
    CONSTRAINT trip_stop_updates_direction_check
        CHECK (direction_id IS NULL OR direction_id IN (0, 1)),
    CONSTRAINT trip_stop_updates_stop_reference_check
        CHECK (stop_id IS NOT NULL OR stop_sequence IS NOT NULL),
    CONSTRAINT trip_stop_updates_stop_sequence_check
        CHECK (stop_sequence IS NULL OR stop_sequence >= 0)
);

COMMIT;

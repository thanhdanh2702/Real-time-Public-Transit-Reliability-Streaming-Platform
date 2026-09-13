BEGIN;

CREATE TABLE staging.vehicle_positions (
    event_id            TEXT                PRIMARY KEY,
    vehicle_id          TEXT                NOT NULL,

    event_timestamp     TIMESTAMPTZ         NOT NULL,
    feed_timestamp      TIMESTAMPTZ         NOT NULL,
    published_at        TIMESTAMPTZ         NOT NULL,
    ingested_at         TIMESTAMPTZ         NOT NULL,

    route_id            TEXT,
    trip_id             TEXT,

    latitude            DOUBLE PRECISION    NOT NULL,
    longitude           DOUBLE PRECISION    NOT NULL,
    bearing             DOUBLE PRECISION,
    odometer            DOUBLE PRECISION,
    speed_mps           DOUBLE PRECISION,
    occupancy_status    TEXT,

    source              TEXT                NOT NULL,
    schema_version      SMALLINT            NOT NULL,
    kafka_topic         TEXT                NOT NULL,
    kafka_partition     INTEGER             NOT NULL,
    kafka_offset        BIGINT              NOT NULL,
    kafka_timestamp     TIMESTAMPTZ         NOT NULL,

    created_at          TIMESTAMPTZ         NOT NULL    DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT latitude_check
        CHECK (latitude BETWEEN -90 AND 90),

    CONSTRAINT longitude_check
        CHECK (longitude BETWEEN -180 AND 180),

    CONSTRAINT bearing_check
        CHECK (bearing IS NULL OR bearing BETWEEN 0 AND 360),

    CONSTRAINT odometer_check
        CHECK (odometer IS NULL OR odometer >= 0),

    CONSTRAINT speed_check
        CHECK (speed_mps IS NULL OR speed_mps >= 0),

    CONSTRAINT schema_version_check
        CHECK (schema_version > 0),

    CONSTRAINT kafka_position_unique
        UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);

COMMIT;

BEGIN;

CREATE TABLE staging.service_alert_entities (
    event_id                    TEXT        NOT NULL,
    informed_entity_index       INTEGER     NOT NULL,
    alert_id                    TEXT        NOT NULL,

    event_timestamp             TIMESTAMPTZ NOT NULL,
    feed_timestamp              TIMESTAMPTZ NOT NULL,
    ingested_at                 TIMESTAMPTZ NOT NULL,
    published_at                TIMESTAMPTZ NOT NULL,

    cause                       TEXT,
    effect                      TEXT,
    severity                    TEXT,
    header_text                 TEXT,
    description_text            TEXT,
    url                         TEXT,
    active_periods              JSONB       NOT NULL,

    agency_id                   TEXT,
    route_id                    TEXT,
    route_type                  INTEGER,
    trip_id                     TEXT,
    stop_id                     TEXT,
    direction_id                INTEGER,

    source                      TEXT        NOT NULL,
    schema_version              SMALLINT    NOT NULL,
    kafka_topic                 TEXT        NOT NULL,
    kafka_partition             INTEGER     NOT NULL,
    kafka_offset                BIGINT      NOT NULL,
    kafka_timestamp             TIMESTAMPTZ NOT NULL,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT service_alert_entities_pk
        PRIMARY KEY (event_id, informed_entity_index),
    CONSTRAINT service_alert_entities_periods_check
        CHECK (jsonb_typeof(active_periods) = 'array'),
    CONSTRAINT service_alert_entities_direction_check
        CHECK (direction_id IS NULL OR direction_id IN (0, 1)),
    CONSTRAINT service_alert_entities_route_type_check
        CHECK (route_type IS NULL OR route_type >= 0)
);

COMMIT;

BEGIN;

-- Current reference feed only. Original ZIPs retain source history on disk.
CREATE TABLE IF NOT EXISTS raw.gtfs_routes (
    route_id            TEXT                PRIMARY KEY,
    agency_id           TEXT,
    route_short_name    TEXT,
    route_long_name     TEXT,
    route_desc          TEXT,
    route_type          INTEGER             NOT NULL,
    route_color         TEXT,
    route_text_color    TEXT,
    route_url           TEXT,
    route_sort_order    INTEGER,
    route_fare_class    TEXT,
    line_id             TEXT,
    listed_route        INTEGER,
    network_id          TEXT,

    CONSTRAINT gtfs_routes_route_id_check
        CHECK (btrim(route_id) <> ''),

    CONSTRAINT gtfs_routes_route_type_check
        CHECK (route_type >= 0)
);

CREATE TABLE IF NOT EXISTS raw.gtfs_stops (
    stop_id             TEXT                PRIMARY KEY,
    stop_code           TEXT,
    stop_name           TEXT,
    stop_desc           TEXT,
    platform_name       TEXT,
    zone_id             TEXT,
    stop_address        TEXT,
    stop_url            TEXT,
    level_id            TEXT,
    municipality        TEXT,
    on_street           TEXT,
    at_street           TEXT,
    vehicle_type        INTEGER,
    stop_lat            DOUBLE PRECISION,
    stop_lon            DOUBLE PRECISION,
    location_type       INTEGER,
    parent_station      TEXT,
    wheelchair_boarding INTEGER,
    platform_code       TEXT,

    CONSTRAINT gtfs_stops_stop_id_check
        CHECK (btrim(stop_id) <> ''),

    CONSTRAINT gtfs_stops_stop_lat_check
        CHECK (stop_lat BETWEEN -90 AND 90),

    CONSTRAINT gtfs_stops_stop_lon_check
        CHECK (stop_lon BETWEEN -180 AND 180),

    CONSTRAINT gtfs_stops_location_type_check
        CHECK (location_type BETWEEN 0 AND 4),

    CONSTRAINT gtfs_stops_wheelchair_boarding_check
        CHECK (wheelchair_boarding BETWEEN 0 AND 2)
);

CREATE TABLE IF NOT EXISTS raw.gtfs_trips (
    trip_id               TEXT                PRIMARY KEY,
    route_id              TEXT                NOT NULL,
    service_id            TEXT                NOT NULL,
    trip_headsign         TEXT,
    trip_short_name       TEXT,
    direction_id          INTEGER,
    block_id              TEXT,
    shape_id              TEXT,
    wheelchair_accessible INTEGER,
    bikes_allowed         INTEGER,
    trip_route_type       INTEGER,
    route_pattern_id      TEXT,

    CONSTRAINT gtfs_trips_trip_id_check
        CHECK (btrim(trip_id) <> ''),

    CONSTRAINT gtfs_trips_route_id_check
        CHECK (btrim(route_id) <> ''),

    CONSTRAINT gtfs_trips_service_id_check
        CHECK (btrim(service_id) <> ''),

    CONSTRAINT gtfs_trips_direction_id_check
        CHECK (direction_id IN (0, 1)),

    CONSTRAINT gtfs_trips_wheelchair_accessible_check
        CHECK (wheelchair_accessible BETWEEN 0 AND 2),

    CONSTRAINT gtfs_trips_bikes_allowed_check
        CHECK (bikes_allowed BETWEEN 0 AND 2)
);

CREATE TABLE IF NOT EXISTS raw.gtfs_stop_times (
    trip_id             TEXT                NOT NULL,
    stop_sequence       INTEGER             NOT NULL,
    stop_id             TEXT                NOT NULL,
    -- GTFS permits hours >= 24; PostgreSQL TIME does not represent these.
    arrival_time        TEXT,
    departure_time      TEXT,
    stop_headsign       TEXT,
    pickup_type         INTEGER,
    drop_off_type       INTEGER,
    timepoint           INTEGER,
    checkpoint_id       TEXT,
    continuous_pickup   INTEGER,
    continuous_drop_off INTEGER,

    CONSTRAINT gtfs_stop_times_trip_id_check
        CHECK (btrim(trip_id) <> ''),

    CONSTRAINT gtfs_stop_times_stop_sequence_check
        CHECK (stop_sequence >= 0),

    CONSTRAINT gtfs_stop_times_stop_id_check
        CHECK (btrim(stop_id) <> ''),

    CONSTRAINT gtfs_stop_times_arrival_time_check
        CHECK (arrival_time ~ '^[0-9]{1,3}:[0-5][0-9]:[0-5][0-9]$'),

    CONSTRAINT gtfs_stop_times_departure_time_check
        CHECK (departure_time ~ '^[0-9]{1,3}:[0-5][0-9]:[0-5][0-9]$'),

    CONSTRAINT gtfs_stop_times_pickup_type_check
        CHECK (pickup_type BETWEEN 0 AND 3),

    CONSTRAINT gtfs_stop_times_drop_off_type_check
        CHECK (drop_off_type BETWEEN 0 AND 3),

    CONSTRAINT gtfs_stop_times_timepoint_check
        CHECK (timepoint IN (0, 1)),

    PRIMARY KEY (trip_id, stop_sequence)
);

CREATE TABLE IF NOT EXISTS raw.gtfs_calendar (
    service_id          TEXT                PRIMARY KEY,
    monday              INTEGER             NOT NULL,
    tuesday             INTEGER             NOT NULL,
    wednesday           INTEGER             NOT NULL,
    thursday            INTEGER             NOT NULL,
    friday              INTEGER             NOT NULL,
    saturday            INTEGER             NOT NULL,
    sunday              INTEGER             NOT NULL,
    start_date          DATE                NOT NULL,
    end_date            DATE                NOT NULL,

    CONSTRAINT gtfs_calendar_service_id_check
        CHECK (btrim(service_id) <> ''),

    CONSTRAINT gtfs_calendar_monday_check
        CHECK (monday IN (0, 1)),

    CONSTRAINT gtfs_calendar_tuesday_check
        CHECK (tuesday IN (0, 1)),

    CONSTRAINT gtfs_calendar_wednesday_check
        CHECK (wednesday IN (0, 1)),

    CONSTRAINT gtfs_calendar_thursday_check
        CHECK (thursday IN (0, 1)),

    CONSTRAINT gtfs_calendar_friday_check
        CHECK (friday IN (0, 1)),

    CONSTRAINT gtfs_calendar_saturday_check
        CHECK (saturday IN (0, 1)),

    CONSTRAINT gtfs_calendar_sunday_check
        CHECK (sunday IN (0, 1)),

    CONSTRAINT gtfs_calendar_date_range_check
        CHECK (end_date >= start_date)
);

CREATE TABLE IF NOT EXISTS raw.gtfs_calendar_dates (
    service_id          TEXT                NOT NULL,
    date                DATE                NOT NULL,
    exception_type      INTEGER             NOT NULL,
    holiday_name        TEXT,

    CONSTRAINT gtfs_calendar_dates_service_id_check
        CHECK (btrim(service_id) <> ''),

    CONSTRAINT gtfs_calendar_dates_exception_type_check
        CHECK (exception_type IN (1, 2)),

    PRIMARY KEY (service_id, date)
);

COMMIT;

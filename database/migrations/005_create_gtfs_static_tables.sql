BEGIN;

-- Current reference feed only. Original ZIPs retain source history on disk.
CREATE TABLE IF NOT EXISTS raw.gtfs_routes (
    route_id TEXT PRIMARY KEY CHECK (btrim(route_id) <> ''),
    agency_id TEXT,
    route_short_name TEXT,
    route_long_name TEXT,
    route_type INTEGER NOT NULL CHECK (route_type >= 0),
    route_color TEXT,
    route_text_color TEXT,
    feed_version TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.gtfs_stops (
    stop_id TEXT PRIMARY KEY CHECK (btrim(stop_id) <> ''),
    stop_code TEXT,
    stop_name TEXT,
    stop_lat DOUBLE PRECISION CHECK (stop_lat BETWEEN -90 AND 90),
    stop_lon DOUBLE PRECISION CHECK (stop_lon BETWEEN -180 AND 180),
    location_type INTEGER CHECK (location_type BETWEEN 0 AND 4),
    parent_station TEXT,
    wheelchair_boarding INTEGER CHECK (wheelchair_boarding BETWEEN 0 AND 2),
    platform_code TEXT,
    feed_version TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.gtfs_trips (
    trip_id TEXT PRIMARY KEY CHECK (btrim(trip_id) <> ''),
    route_id TEXT NOT NULL CHECK (btrim(route_id) <> ''),
    service_id TEXT NOT NULL CHECK (btrim(service_id) <> ''),
    trip_headsign TEXT,
    trip_short_name TEXT,
    direction_id INTEGER CHECK (direction_id IN (0, 1)),
    block_id TEXT,
    shape_id TEXT,
    wheelchair_accessible INTEGER CHECK (wheelchair_accessible BETWEEN 0 AND 2),
    bikes_allowed INTEGER CHECK (bikes_allowed BETWEEN 0 AND 2),
    feed_version TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.gtfs_stop_times (
    trip_id TEXT NOT NULL CHECK (btrim(trip_id) <> ''),
    stop_sequence INTEGER NOT NULL CHECK (stop_sequence >= 0),
    stop_id TEXT NOT NULL CHECK (btrim(stop_id) <> ''),
    -- GTFS permits hours >= 24; PostgreSQL TIME does not represent these.
    arrival_time TEXT CHECK (arrival_time ~ '^[0-9]{1,3}:[0-5][0-9]:[0-5][0-9]$'),
    departure_time TEXT CHECK (departure_time ~ '^[0-9]{1,3}:[0-5][0-9]:[0-5][0-9]$'),
    stop_headsign TEXT,
    pickup_type INTEGER CHECK (pickup_type BETWEEN 0 AND 3),
    drop_off_type INTEGER CHECK (drop_off_type BETWEEN 0 AND 3),
    timepoint INTEGER CHECK (timepoint IN (0, 1)),
    feed_version TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (trip_id, stop_sequence)
);

CREATE TABLE IF NOT EXISTS raw.gtfs_calendar (
    service_id TEXT PRIMARY KEY CHECK (btrim(service_id) <> ''),
    monday INTEGER NOT NULL CHECK (monday IN (0, 1)),
    tuesday INTEGER NOT NULL CHECK (tuesday IN (0, 1)),
    wednesday INTEGER NOT NULL CHECK (wednesday IN (0, 1)),
    thursday INTEGER NOT NULL CHECK (thursday IN (0, 1)),
    friday INTEGER NOT NULL CHECK (friday IN (0, 1)),
    saturday INTEGER NOT NULL CHECK (saturday IN (0, 1)),
    sunday INTEGER NOT NULL CHECK (sunday IN (0, 1)),
    start_date DATE NOT NULL,
    end_date DATE NOT NULL CHECK (end_date >= start_date),
    feed_version TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.gtfs_calendar_dates (
    service_id TEXT NOT NULL CHECK (btrim(service_id) <> ''),
    date DATE NOT NULL,
    exception_type INTEGER NOT NULL CHECK (exception_type IN (1, 2)),
    feed_version TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (service_id, date)
);

COMMIT;

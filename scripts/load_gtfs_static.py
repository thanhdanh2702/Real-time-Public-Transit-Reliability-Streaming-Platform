"""Load one MBTA GTFS ZIP as the current reference feed, atomically."""

import argparse
import csv
import os
import warnings
from datetime import datetime
from io import TextIOWrapper
from pathlib import Path
from zipfile import ZipFile

import psycopg
from psycopg import sql

# Selected project fields, not every optional MBTA extension. Extra fields stay in the ZIP.
COLUMNS = {
    "routes": (
        "route_id agency_id route_short_name route_long_name "
        "route_type route_color route_text_color"
    ),
    "stops": (
        "stop_id stop_code stop_name stop_lat stop_lon location_type "
        "parent_station wheelchair_boarding platform_code"
    ),
    "trips": (
        "trip_id route_id service_id trip_headsign trip_short_name direction_id "
        "block_id shape_id wheelchair_accessible bikes_allowed"
    ),
    "stop_times": (
        "trip_id stop_sequence stop_id arrival_time departure_time "
        "stop_headsign pickup_type drop_off_type timepoint"
    ),
    "calendar": (
        "service_id monday tuesday wednesday thursday friday saturday sunday start_date end_date"
    ),
    "calendar_dates": "service_id date exception_type",
}
REQUIRED = {
    "routes": ("route_id", "route_type"),
    "stops": ("stop_id",),
    "trips": ("trip_id", "route_id", "service_id"),
    "stop_times": ("trip_id", "stop_sequence", "stop_id"),
    "calendar": tuple(COLUMNS["calendar"].split()),
    "calendar_dates": tuple(COLUMNS["calendar_dates"].split()),
}


def read_csv(archive, name, required):
    """Stream rows directly from ZIP; never extract paths or read the whole file into RAM."""
    filename = f"{name}.txt"
    if archive.namelist().count(filename) != 1:
        raise ValueError(f"Expected exactly one {filename} at ZIP root")
    with (
        archive.open(filename) as source,
        TextIOWrapper(source, encoding="utf-8-sig", newline="") as text,
    ):
        reader = csv.DictReader(text, strict=True)
        fields = reader.fieldnames or []
        if len(fields) != len(set(fields)) or not set(required) <= set(fields):
            raise ValueError(f"Invalid header in {filename}; required: {required}")
        for row in reader:
            if None in row or None in row.values():
                raise ValueError(f"Wrong column count in {filename}, line {reader.line_num}")
            yield row


def connection_from_env():
    """Credentials come from the environment, never from code or CLI arguments."""
    password = os.environ.get("POSTGRES_PASSWORD")
    if not password:
        raise ValueError("POSTGRES_PASSWORD is required")
    return {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", os.getenv("POSTGRES_HOST_PORT", "5432"))),
        "dbname": os.getenv("POSTGRES_DB", "transitpulse"),
        "user": os.getenv("POSTGRES_USER", "transitpulse_admin"),
        "password": password,
        "connect_timeout": 10,
    }


def validate_references(db):
    """Validate relations in the temporary feed, including calendar_dates-only services."""
    checks = {
        "trips.route_id": (
            "SELECT 1 FROM new_trips t LEFT JOIN new_routes r USING(route_id) "
            "WHERE r.route_id IS NULL"
        ),
        "stop_times.trip_id": (
            "SELECT 1 FROM new_stop_times s LEFT JOIN new_trips t USING(trip_id) "
            "WHERE t.trip_id IS NULL"
        ),
        "stop_times.stop_id": (
            "SELECT 1 FROM new_stop_times s LEFT JOIN new_stops t USING(stop_id) "
            "WHERE t.stop_id IS NULL"
        ),
        "trips.service_id": (
            "SELECT 1 FROM new_trips t LEFT JOIN (SELECT service_id FROM new_calendar "
            "UNION SELECT service_id FROM new_calendar_dates WHERE exception_type = 1) "
            "c USING(service_id) WHERE c.service_id IS NULL"
        ),
        "stops.parent_station": (
            "SELECT 1 FROM new_stops s LEFT JOIN new_stops p ON s.parent_station = p.stop_id "
            "WHERE s.parent_station IS NOT NULL AND p.stop_id IS NULL"
        ),
    }
    for label, query in checks.items():
        if db.execute(query + " LIMIT 1").fetchone():
            raise ValueError(f"Broken GTFS reference: {label}")


def load_gtfs(path, connection, *, schema="raw"):
    """Replace only the six GTFS tables after every file passes checks."""
    counts = {}
    with ZipFile(path) as archive:
        if sum(item.file_size for item in archive.infolist()) > 1_000_000_000:
            raise ValueError("GTFS ZIP exceeds 1 GB uncompressed limit")
        info = list(read_csv(archive, "feed_info", ("feed_version", "feed_end_date")))
        if len(info) != 1 or not info[0]["feed_version"].strip():
            raise ValueError("Expected one feed_info row with a non-empty feed_version")
        if datetime.strptime(info[0]["feed_end_date"], "%Y%m%d").date() < datetime.now().date():
            warnings.warn("GTFS feed has expired; use for import tests only", stacklevel=2)
        with psycopg.connect(**connection) as db:
            # Serialize loaders targeting the same schema. Released on commit/rollback.
            db.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"gtfs:{schema}",))
            for name, field_list in COLUMNS.items():
                columns = field_list.split()
                temp = sql.Identifier(f"new_{name}")
                target = sql.Identifier(schema, f"gtfs_{name}")
                db.execute(
                    sql.SQL("CREATE TEMP TABLE {} (LIKE {} INCLUDING ALL) ON COMMIT DROP").format(
                        temp, target
                    )
                )
                statement = sql.SQL("COPY {} ({}) FROM STDIN").format(
                    temp, sql.SQL(", ").join(map(sql.Identifier, [*columns, "feed_version"]))
                )
                counts[name] = 0
                with db.cursor().copy(statement) as copy:
                    for row in read_csv(archive, name, REQUIRED[name]):
                        copy.write_row(
                            [row.get(c) or None for c in columns] + [info[0]["feed_version"]]
                        )
                        counts[name] += 1
                if not counts[name] and name not in ("calendar", "calendar_dates"):
                    raise ValueError(f"Empty required table: {name}")
            validate_references(db)
            for name in COLUMNS:
                target = sql.Identifier(schema, f"gtfs_{name}")
                # DELETE preserves table identity and MVCC readers; no CASCADE/TRUNCATE.
                db.execute(sql.SQL("DELETE FROM {}").format(target))
                db.execute(
                    sql.SQL("INSERT INTO {} SELECT * FROM {}").format(
                        target, sql.Identifier(f"new_{name}")
                    )
                )
    return counts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("zip_path", type=Path)
    args = parser.parse_args()
    counts = load_gtfs(args.zip_path, connection_from_env())
    for name, count in counts.items():
        print(f"raw.gtfs_{name}: {count:,} rows")
    print("Committed all six GTFS tables.")


if __name__ == "__main__":
    main()

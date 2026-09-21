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

REQUIRED = {
    "routes": ("route_id", "route_type"),
    "stops": ("stop_id",),
    "trips": ("trip_id", "route_id", "service_id"),
    "stop_times": ("trip_id", "stop_sequence", "stop_id"),
    "calendar": (
        "service_id",
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday",
        "start_date",
        "end_date",
    ),
    "calendar_dates": ("service_id", "date", "exception_type"),
}


def read_csv(archive, name, required):
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


def validate_references(db, schema):
    checks = {
        "trips.route_id": (
            "SELECT 1 FROM {trips} t LEFT JOIN {routes} r USING(route_id) WHERE r.route_id IS NULL"
        ),
        "stop_times.trip_id": (
            "SELECT 1 FROM {stop_times} s LEFT JOIN {trips} t USING(trip_id) "
            "WHERE t.trip_id IS NULL"
        ),
        "stop_times.stop_id": (
            "SELECT 1 FROM {stop_times} s LEFT JOIN {stops} t USING(stop_id) "
            "WHERE t.stop_id IS NULL"
        ),
        "trips.service_id": (
            "SELECT 1 FROM {trips} t LEFT JOIN (SELECT service_id FROM {calendar} "
            "UNION SELECT service_id FROM {calendar_dates} WHERE exception_type = 1) "
            "c USING(service_id) WHERE c.service_id IS NULL"
        ),
        "stops.parent_station": (
            "SELECT 1 FROM {stops} s LEFT JOIN {stops} p ON s.parent_station = p.stop_id "
            "WHERE NULLIF(s.parent_station, '') IS NOT NULL AND p.stop_id IS NULL"
        ),
    }
    for label, query in checks.items():
        statement = sql.SQL(query + " LIMIT 1").format(
            **{name: sql.Identifier(schema, f"gtfs_{name}") for name in REQUIRED}
        )
        if db.execute(statement).fetchone():
            raise ValueError(f"Broken GTFS reference: {label}")


def read_header(archive, name):
    filename = f"{name}.txt"
    if archive.namelist().count(filename) != 1:
        raise ValueError(f"Expected exactly one {filename} at ZIP root")
    with (
        archive.open(filename) as source,
        TextIOWrapper(source, encoding="utf-8-sig", newline="") as text,
    ):
        fields = next(csv.reader(text, strict=True), [])
    if (
        not fields
        or any(not field for field in fields)
        or len(fields) != len(set(fields))
        or not set(REQUIRED[name]) <= set(fields)
    ):
        raise ValueError(f"Invalid header in {filename}")
    return fields


def load_gtfs(path, connection, *, schema="raw"):
    counts = {}
    with ZipFile(path) as archive:
        if sum(item.file_size for item in archive.infolist()) > 1_000_000_000:
            raise ValueError("GTFS ZIP exceeds 1 GB uncompressed limit")
        info = list(read_csv(archive, "feed_info", ("feed_version", "feed_end_date")))
        if len(info) != 1 or not info[0]["feed_version"].strip():
            raise ValueError("Expected one feed_info row with a non-empty feed_version")
        if datetime.strptime(info[0]["feed_end_date"], "%Y%m%d").date() < datetime.now().date():
            warnings.warn("GTFS feed has expired; use for import tests only", stacklevel=2)
        headers = {name: read_header(archive, name) for name in REQUIRED}
        with psycopg.connect(**connection) as db:
            db.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"gtfs:{schema}",))
            # Reject schema drift rather than silently discarding source columns.
            for name, columns in headers.items():
                target_columns = {
                    row[0]
                    for row in db.execute(
                        "SELECT column_name FROM information_schema.columns "
                        "WHERE table_schema = %s AND table_name = %s",
                        (schema, f"gtfs_{name}"),
                    )
                }
                if not target_columns or set(columns) - target_columns:
                    raise ValueError(
                        f"Schema mismatch for {schema}.gtfs_{name}; use migration 005 "
                        f"and ensure every CSV column exists: "
                        f"{sorted(set(columns) - target_columns)}"
                    )
            for name, columns in headers.items():
                target = sql.Identifier(schema, f"gtfs_{name}")
                db.execute(sql.SQL("DELETE FROM {}").format(target))
                statement = sql.SQL(
                    "COPY {} ({}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)"
                ).format(target, sql.SQL(", ").join(map(sql.Identifier, columns)))
                with (
                    archive.open(f"{name}.txt") as source,
                    TextIOWrapper(source, encoding="utf-8-sig", newline="") as text,
                    db.cursor() as cur,
                ):
                    with cur.copy(statement) as copy:
                        while chunk := text.read(64 * 1024):
                            copy.write(chunk)
                    counts[name] = cur.rowcount
                if not counts[name] and name not in ("calendar", "calendar_dates"):
                    raise ValueError(f"Empty required table: {name}")
            validate_references(db, schema)
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

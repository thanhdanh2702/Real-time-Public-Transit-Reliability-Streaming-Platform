"""Small GTFS fixtures; database tests use a disposable schema only."""

import os
from pathlib import Path
from uuid import uuid4
from zipfile import ZipFile

import pytest

FILES = {
    "feed_info": "feed_version,feed_start_date,feed_end_date\ntest-v1,20260101,20261231\n",
    "routes": "route_id,route_type,route_long_name\n001,3,Test bus\n",
    "stops": "stop_id,stop_name,stop_lat,stop_lon\n0002,Test stop,42,-71\n",
    "trips": "route_id,service_id,trip_id\n001,S,T\n",
    "stop_times": (
        "trip_id,stop_id,stop_sequence,arrival_time,departure_time\nT,0002,1,25:10:00,25:11:00\n"
    ),
    "calendar": (
        "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
        "S,1,1,1,1,1,0,0,20260101,20261231\n"
    ),
    "calendar_dates": "service_id,date,exception_type\nS,20260201,2\n",
}


def make_zip(tmp_path, **changes):
    path = tmp_path / "feed.zip"
    with ZipFile(path, "w") as archive:
        for name, text in (FILES | changes).items():
            if text is not None:
                archive.writestr(f"{name}.txt", text)
    return path


def test_read_csv_preserves_ids_and_extended_hours(tmp_path):
    from scripts.load_gtfs_static import read_csv

    with ZipFile(make_zip(tmp_path)) as archive:
        rows = list(read_csv(archive, "stop_times", ("trip_id", "stop_id")))
    assert rows[0]["stop_id"] == "0002"
    assert rows[0]["arrival_time"] == "25:10:00"


@pytest.mark.parametrize("text", ["wrong\nvalue\n", "trip_id,trip_id\nT,T\n", "trip_id\nT,extra\n"])
def test_rejects_bad_csv(tmp_path, text):
    from scripts.load_gtfs_static import read_csv

    with ZipFile(make_zip(tmp_path, trips=text)) as archive, pytest.raises(ValueError):
        list(read_csv(archive, "trips", ("trip_id",)))


@pytest.fixture
def database():
    if os.getenv("RUN_POSTGRES_INTEGRATION") != "1":
        pytest.skip("Set RUN_POSTGRES_INTEGRATION=1 for PostgreSQL tests")
    import psycopg
    from psycopg import sql

    from scripts.load_gtfs_static import connection_from_env

    schema = f"test_gtfs_{uuid4().hex}"
    connection = connection_from_env()
    with psycopg.connect(**connection, autocommit=True) as db:
        db.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        try:
            path = Path(__file__).resolve().parents[1] / "database/migrations/005_create_gtfs_static_tables.sql"
            db.execute(path.read_text().replace("raw.", f"{schema}."))
            yield connection, schema, db
        finally:
            db.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


@pytest.mark.integration
def test_load_replay_and_rollback(tmp_path, database):
    from scripts.load_gtfs_static import load_gtfs

    connection, schema, db = database
    path = make_zip(tmp_path)
    for _ in range(2):
        assert load_gtfs(path, connection, schema=schema) == dict.fromkeys(
            ("routes", "stops", "trips", "stop_times", "calendar", "calendar_dates"), 1
        )
    before = db.execute(f"SELECT * FROM {schema}.gtfs_stop_times").fetchall()
    assert len(before) == 1
    assert "25:10:00" in before[0]
    assert "0002" in before[0]
    # A changed parent must not replace the previous feed if the last file fails.
    path = make_zip(tmp_path, calendar_dates="service_id,date,exception_type\nS,20260201,7\n")
    with pytest.raises(Exception):
        load_gtfs(path, connection, schema=schema)
    assert db.execute(f"SELECT * FROM {schema}.gtfs_stop_times").fetchall() == before


@pytest.mark.integration
@pytest.mark.parametrize("changes", [
    {"trips": "route_id,service_id,trip_id\nmissing,S,T\n"},
    {"stops": "stop_id,stop_name\n0002,A\n0002,B\n"},
    {"calendar": None},
    {"routes": "route_id,route_type\n"},
])
def test_invalid_feed_leaves_database_empty(tmp_path, database, changes):
    from scripts.load_gtfs_static import load_gtfs

    connection, schema, db = database
    with pytest.raises(Exception):
        load_gtfs(make_zip(tmp_path, **changes), connection, schema=schema)
    assert db.execute(f"SELECT count(*) FROM {schema}.gtfs_routes").fetchone()[0] == 0

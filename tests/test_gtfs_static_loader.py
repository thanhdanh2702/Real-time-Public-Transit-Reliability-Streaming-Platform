"""Small GTFS fixtures; database tests use a disposable schema only."""

import csv
import os
from io import TextIOWrapper
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


def test_connection_env_and_cli(tmp_path, monkeypatch, capsys):
    from scripts import load_gtfs_static as loader

    monkeypatch.delenv("POSTGRES_PASSWORD", raising=False)
    with pytest.raises(ValueError, match="POSTGRES_PASSWORD"):
        loader.connection_from_env()
    monkeypatch.setenv("POSTGRES_PASSWORD", "test-only")
    monkeypatch.setenv("POSTGRES_PORT", "5433")
    assert loader.connection_from_env()["port"] == 5433
    monkeypatch.setattr("sys.argv", ["loader", str(tmp_path / "feed.zip")])
    monkeypatch.setattr(loader, "load_gtfs", lambda path, connection: {"routes": 1})
    loader.main()
    assert "raw.gtfs_routes: 1 rows" in capsys.readouterr().out


@pytest.mark.parametrize(
    "text", ["feed_version,feed_end_date\n", "feed_version,feed_end_date\n,20261231\n"]
)
def test_invalid_metadata_before_connection(tmp_path, text):
    from scripts.load_gtfs_static import load_gtfs

    with pytest.raises(ValueError, match="feed_info"):
        load_gtfs(make_zip(tmp_path, feed_info=text), {})


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
            path = (
                Path(__file__).resolve().parents[1]
                / "database/migrations/005_create_gtfs_static_tables.sql"
            )
            db.execute(path.read_text().replace("raw.", f"{schema}."))
            yield connection, schema, db
        finally:
            db.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


@pytest.mark.integration
def test_load_replay_and_rollback(tmp_path, database):
    import psycopg

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
    with pytest.raises(psycopg.errors.CheckViolation):
        load_gtfs(path, connection, schema=schema)
    assert db.execute(f"SELECT * FROM {schema}.gtfs_stop_times").fetchall() == before


@pytest.mark.integration
@pytest.mark.parametrize(
    "changes",
    [
        {"trips": "route_id,service_id,trip_id\nmissing,S,T\n"},
        {"stops": "stop_id,stop_name\n0002,A\n0002,B\n"},
        {"calendar": None},
        {"routes": "route_id,route_type\n"},
        {"stop_times": "trip_id,stop_id,stop_sequence\nmissing,0002,1\n"},
        {"stop_times": "trip_id,stop_id,stop_sequence\nT,missing,1\n"},
        {"trips": "route_id,service_id,trip_id\n001,missing,T\n"},
        {"stops": "stop_id,parent_station\n0002,missing\n"},
    ],
)
def test_invalid_feed_leaves_database_empty(tmp_path, database, changes):
    import psycopg

    from scripts.load_gtfs_static import load_gtfs

    connection, schema, db = database
    with pytest.raises((ValueError, psycopg.errors.UniqueViolation)):
        load_gtfs(make_zip(tmp_path, **changes), connection, schema=schema)
    assert db.execute(f"SELECT count(*) FROM {schema}.gtfs_routes").fetchone()[0] == 0


@pytest.mark.integration
def test_calendar_dates_only_and_expired_warning(tmp_path, database):
    from scripts.load_gtfs_static import load_gtfs

    connection, schema, _ = database
    path = make_zip(
        tmp_path,
        calendar=FILES["calendar"].splitlines()[0] + "\n",
        calendar_dates="service_id,date,exception_type\nS,20260201,1\n",
        feed_info="feed_version,feed_end_date\nexpired,20000101\n",
    )
    with pytest.warns(UserWarning, match="expired"):
        assert load_gtfs(path, connection, schema=schema)["calendar"] == 0


@pytest.mark.integration
def test_block_copy_preserves_source_columns_and_csv_quoting(tmp_path, database):
    from scripts.load_gtfs_static import load_gtfs

    connection, schema, db = database
    path = make_zip(
        tmp_path,
        routes="\ufeffroute_desc,route_type,route_id,network_id\r\n"
        '"Bus, ""express""\nBoston",3,001,rapid_transit\r\n',
        stops='stop_id,stop_name,municipality,parent_station\n0002,"",Boston,""\n',
        trips="trip_id,route_id,service_id,route_pattern_id\nT,001,S,001-_-0\n",
        stop_times="trip_id,stop_sequence,stop_id,arrival_time,checkpoint_id\n"
        "T,1,0002,25:10:00,matt\n",
        calendar_dates="service_id,date,exception_type,holiday_name\n"
        'S,20260201,2,"Ngày lễ, Boston"\n',
    )
    assert load_gtfs(path, connection, schema=schema)["routes"] == 1
    assert db.execute(
        f"SELECT route_id, route_desc, network_id FROM {schema}.gtfs_routes"
    ).fetchone() == ("001", 'Bus, "express"\nBoston', "rapid_transit")
    assert db.execute(
        f"SELECT stop_name, municipality, parent_station FROM {schema}.gtfs_stops"
    ).fetchone() == ("", "Boston", "")
    assert db.execute(f"SELECT route_pattern_id FROM {schema}.gtfs_trips").fetchone() == (
        "001-_-0",
    )
    assert db.execute(f"SELECT checkpoint_id FROM {schema}.gtfs_stop_times").fetchone() == ("matt",)
    assert db.execute(f"SELECT holiday_name FROM {schema}.gtfs_calendar_dates").fetchone() == (
        "Ngày lễ, Boston",
    )


@pytest.mark.integration
@pytest.mark.parametrize(
    "changes",
    [
        {"calendar_dates": "service_id,date,exception_type\nS,20260201,7\n"},
        {"calendar_dates": "service_id,date,exception_type\nS,not-a-date,2\n"},
        {"calendar_dates": "service_id,date,exception_type\nS,20260201,2,extra\n"},
        {"calendar_dates": "service_id,date,exception_type\nS,20260201\n"},
        {"calendar_dates": 'service_id,date,exception_type\n"unterminated'},
        {"trips": "route_id,service_id,trip_id\nmissing,S,T\n"},
        {"stops": "stop_id\n0002\n0002\n"},
        {"routes": "route_id,route_type,unknown_column\n001,3,value\n"},
        {"routes": "route_id,route_type,route_id\n001,3,001\n"},
        {"routes": "route_id,route_type\n"},
        {"calendar": None},
    ],
)
def test_failed_refresh_preserves_all_six_tables(tmp_path, database, changes):
    import psycopg

    from scripts.load_gtfs_static import REQUIRED, load_gtfs

    connection, schema, db = database
    load_gtfs(make_zip(tmp_path), connection, schema=schema)
    before = {
        name: db.execute(f"SELECT * FROM {schema}.gtfs_{name}").fetchall() for name in REQUIRED
    }
    with pytest.raises((ValueError, psycopg.Error)):
        load_gtfs(make_zip(tmp_path, **changes), connection, schema=schema)
    for name, rows in before.items():
        assert db.execute(f"SELECT * FROM {schema}.gtfs_{name}").fetchall() == rows


@pytest.mark.integration
def test_refresh_removes_obsolete_rows(tmp_path, database):
    from scripts.load_gtfs_static import load_gtfs

    connection, schema, db = database
    load_gtfs(
        make_zip(tmp_path, routes="route_id,route_type\n001,3\nOLD,3\n"), connection, schema=schema
    )
    load_gtfs(make_zip(tmp_path), connection, schema=schema)
    assert db.execute(f"SELECT route_id FROM {schema}.gtfs_routes").fetchall() == [("001",)]


@pytest.mark.integration
def test_schema_creation_is_repeatable_and_preserves_rows(tmp_path, database):
    from scripts.load_gtfs_static import REQUIRED, load_gtfs

    connection, schema, db = database
    load_gtfs(make_zip(tmp_path), connection, schema=schema)
    before = {
        name: db.execute(f"SELECT * FROM {schema}.gtfs_{name}").fetchall() for name in REQUIRED
    }
    migration = (
        Path(__file__).resolve().parents[1]
        / "database/migrations/005_create_gtfs_static_tables.sql"
    ).read_text()
    for _ in range(2):
        db.execute(migration.replace("raw.", f"{schema}."))
    for name, rows in before.items():
        assert db.execute(f"SELECT * FROM {schema}.gtfs_{name}").fetchall() == rows
    load_gtfs(
        make_zip(tmp_path, routes="route_id,route_type,network_id\n001,3,bus\n"),
        connection,
        schema=schema,
    )
    assert db.execute(f"SELECT network_id FROM {schema}.gtfs_routes").fetchone() == ("bus",)


@pytest.mark.integration
def test_real_mbta_zip_matches_schema_and_loads(database):
    from scripts.load_gtfs_static import REQUIRED, load_gtfs

    zip_path = os.getenv("GTFS_TEST_ZIP")
    if not zip_path:
        pytest.skip("Set GTFS_TEST_ZIP for full-feed import")
    connection, schema, db = database
    with ZipFile(zip_path) as archive:
        for name in REQUIRED:
            with (
                archive.open(f"{name}.txt") as source,
                TextIOWrapper(source, encoding="utf-8-sig", newline="") as text,
            ):
                header = next(csv.reader(text))
            columns = {
                r[0]
                for r in db.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name = %s",
                    (schema, f"gtfs_{name}"),
                )
            }
            assert columns == set(header)
    for _ in range(2):
        counts = load_gtfs(zip_path, connection, schema=schema)
        with ZipFile(zip_path) as archive:
            for name in REQUIRED:
                with (
                    archive.open(f"{name}.txt") as source,
                    TextIOWrapper(source, encoding="utf-8-sig", newline="") as text,
                ):
                    reader = csv.reader(text)
                    next(reader)
                    expected = sum(1 for _ in reader)
                assert counts[name] == expected
                assert (
                    db.execute(f"SELECT count(*) FROM {schema}.gtfs_{name}").fetchone()[0]
                    == expected
                )

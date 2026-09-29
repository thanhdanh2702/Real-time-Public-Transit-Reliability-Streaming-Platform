import pytest
from sqlalchemy import create_engine

from dashboard.queries import database


def test_database_engine_uses_environment_variables(monkeypatch):
    monkeypatch.setenv("POSTGRES_USER", "dashboard_user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "p@ss:/word")
    monkeypatch.setenv("POSTGRES_HOST", "postgres")
    monkeypatch.setenv("POSTGRES_PORT", "5432")
    monkeypatch.setenv("POSTGRES_DB", "transitpulse")

    engine = database.get_database_engine()

    assert engine is database.get_database_engine()
    assert engine.url.drivername == "postgresql+psycopg"
    assert engine.url.username == "dashboard_user"
    assert engine.url.password == "p@ss:/word"
    assert engine.url.host == "postgres"
    assert engine.url.port == 5432
    assert engine.url.database == "transitpulse"
    engine.dispose()
    database.get_database_engine.cache_clear()


def test_database_engine_requires_credentials(monkeypatch):
    monkeypatch.setenv("POSTGRES_USER", "dashboard_user")
    monkeypatch.delenv("POSTGRES_PASSWORD", raising=False)

    with pytest.raises(RuntimeError, match="POSTGRES_PASSWORD"):
        database.get_database_engine()


def test_read_dataframe_binds_parameters(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    monkeypatch.setattr(database, "get_database_engine", lambda: engine)

    with engine.begin() as connection:
        connection.exec_driver_sql("CREATE TABLE routes (route_id TEXT)")
        connection.exec_driver_sql("INSERT INTO routes (route_id) VALUES ('1')")

    result = database.read_dataframe(
        "SELECT route_id FROM routes WHERE route_id = :route_id",
        {"route_id": "1' OR 1=1 --"},
    )

    assert result.empty
    assert result.columns.tolist() == ["route_id"]
    engine.dispose()


def test_read_dataframe_returns_query_rows(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    monkeypatch.setattr(database, "get_database_engine", lambda: engine)

    result = database.read_dataframe("SELECT 1 AS vehicle_count")

    assert result.to_dict("records") == [{"vehicle_count": 1}]
    engine.dispose()

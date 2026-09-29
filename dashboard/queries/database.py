import os
from functools import lru_cache

import pandas as pd
from sqlalchemy import URL, Engine, create_engine, text


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


@lru_cache(maxsize=1)
def get_database_engine() -> Engine:
    """Create one reusable connection pool for the dashboard process."""
    database_url = URL.create(
        "postgresql+psycopg",
        username=_required_env("POSTGRES_USER"),
        password=_required_env("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=os.getenv("POSTGRES_DB", "transitpulse"),
    )
    return create_engine(database_url, pool_pre_ping=True)


def read_dataframe(
    query: str,
    parameters: dict[str, object] | None = None,
) -> pd.DataFrame:
    """Run a parameterized query and return its rows as a DataFrame."""
    with get_database_engine().connect() as connection:
        return pd.read_sql_query(text(query), connection, params=parameters)

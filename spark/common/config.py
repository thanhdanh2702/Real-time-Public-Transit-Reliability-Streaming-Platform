import os


def postgres_connection_from_env() -> dict[str, str | int]:
    """Validate database configuration before starting a Spark session."""
    port = int(os.getenv("POSTGRES_PORT", "5432"))
    if not 1 <= port <= 65535:
        raise ValueError("POSTGRES_PORT must be between 1 and 65535")

    password = os.environ["POSTGRES_PASSWORD"]
    if not password:
        raise ValueError("POSTGRES_PASSWORD must not be empty")

    return {
        "host": os.getenv("POSTGRES_HOST", "postgres"),
        "port": port,
        "dbname": os.getenv("POSTGRES_DB", "transitpulse"),
        "user": os.getenv("POSTGRES_USER", "transitpulse_admin"),
        "password": password,
    }

from unittest.mock import MagicMock

import pytest

from spark.jobs.route_reliability import main as trip
from spark.jobs.service_alert import main as alert
from spark.jobs.vehicle_state import main as vehicle


@pytest.mark.parametrize("job", [vehicle, trip, alert], ids=["vehicle", "trip", "alert"])
@pytest.mark.parametrize("port", ["not-a-port", "0", "65536"])
def test_invalid_port_fails_before_spark(monkeypatch, job, port):
    monkeypatch.setenv("POSTGRES_PASSWORD", "test-password")
    monkeypatch.setenv("POSTGRES_PORT", port)
    create_session = MagicMock()
    monkeypatch.setattr(job, "create_spark_session", create_session)

    with pytest.raises(ValueError):
        job.main()

    create_session.assert_not_called()


@pytest.mark.parametrize("job", [vehicle, trip, alert], ids=["vehicle", "trip", "alert"])
@pytest.mark.parametrize("password", [None, ""])
def test_missing_password_fails_before_spark(monkeypatch, job, password):
    monkeypatch.setenv("POSTGRES_PORT", "5432")
    monkeypatch.delenv("POSTGRES_PASSWORD", raising=False)
    if password is not None:
        monkeypatch.setenv("POSTGRES_PASSWORD", password)
    create_session = MagicMock()
    monkeypatch.setattr(job, "create_spark_session", create_session)
    # Prevent any real source/sink call if configuration validation regresses.
    monkeypatch.setattr(job, "read_kafka_stream", MagicMock(side_effect=AssertionError("too late")))

    with pytest.raises((KeyError, ValueError), match="POSTGRES_PASSWORD"):
        job.main()

    create_session.assert_not_called()

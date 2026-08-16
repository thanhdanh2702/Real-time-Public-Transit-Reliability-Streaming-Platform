from unittest.mock import MagicMock

import spark.common.session as session_module


def test_create_spark_session_uses_application_name(monkeypatch) -> None:
    expected_session = object()
    builder = MagicMock()
    builder.appName.return_value = builder
    builder.getOrCreate.return_value = expected_session

    spark_session_class = MagicMock()
    spark_session_class.builder = builder
    monkeypatch.setattr(session_module, "SparkSession", spark_session_class)

    result = session_module.create_spark_session("transitpulse-vehicle-state")

    builder.appName.assert_called_once_with("transitpulse-vehicle-state")
    builder.getOrCreate.assert_called_once_with()
    assert result is expected_session

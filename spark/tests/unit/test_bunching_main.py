import runpy
from unittest.mock import Mock

import pytest

from spark.jobs.route_reliability import main as job


@pytest.mark.parametrize("fails", [False, True])
def test_main_wires_single_query_and_closes_session(monkeypatch, tmp_path, fails):
    spark, raw, query = Mock(), Mock(), Mock()
    monkeypatch.setenv("TRIP_UPDATE_CHECKPOINT_LOCATION", str(tmp_path))
    monkeypatch.setenv("KAFKA_TRIP_UPDATES_TOPIC", "trip.test")
    monkeypatch.setattr(job, "create_spark_session", Mock(return_value=spark))
    source = Mock(return_value=raw)
    start = Mock(return_value=query)
    monkeypatch.setattr(job, "read_kafka_stream", source)
    monkeypatch.setattr(job, "start_bunching_query", start)
    if fails:
        query.awaitTermination.side_effect = RuntimeError("query failed")
        with pytest.raises(RuntimeError, match="query failed"):
            job.main()
    else:
        job.main()
    assert source.call_args.kwargs["topic"] == "trip.test"
    assert source.call_args.kwargs["max_offsets_per_trigger"] == 10000
    assert start.call_args.args[:3] == (spark, raw, tmp_path)
    spark.stop.assert_called_once()


def test_main_rejects_cloud_path_before_starting_spark(monkeypatch):
    monkeypatch.setenv("TRIP_UPDATE_CHECKPOINT_LOCATION", "s3://bucket/checkpoint")
    with pytest.raises(ValueError, match="filesystem"):
        job.main()


def test_bunching_entry_point_delegates_to_same_job(monkeypatch):
    run = Mock()
    monkeypatch.setattr(job, "main", run)
    runpy.run_module("spark.jobs.bunching_detector.main", run_name="__main__")
    run.assert_called_once_with()

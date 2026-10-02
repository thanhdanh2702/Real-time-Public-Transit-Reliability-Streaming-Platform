"""Refresh orchestration tests use in-memory HTTP responses and no database."""

import hashlib
import json
import os
import subprocess
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path
from urllib.error import HTTPError, URLError
from zipfile import BadZipFile, ZipFile
from zoneinfo import ZoneInfo

import pytest

from scripts import refresh_gtfs_static as refresh


def feed_bytes(*, expired=False, stop_name="Central"):
    today = datetime.now(ZoneInfo("America/New_York")).date()
    start = (today - timedelta(days=30)).strftime("%Y%m%d")
    end = (today + timedelta(days=-1 if expired else 30)).strftime("%Y%m%d")
    files = {
        "feed_info": f"feed_version,feed_start_date,feed_end_date\nv1,{start},{end}\n",
        "routes": "route_id,route_type\nR,3\n",
        "stops": f"stop_id,stop_name\nS,{stop_name}\n",
        "trips": "trip_id,route_id,service_id\nT,R,C\n",
        "stop_times": "trip_id,stop_id,stop_sequence\nT,S,1\n",
        "calendar": (
            "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
            f"C,1,1,1,1,1,1,1,{start},{end}\n"
        ),
        "calendar_dates": "service_id,date,exception_type\n",
    }
    buffer = BytesIO()
    with ZipFile(buffer, "w") as archive:
        for name, content in files.items():
            archive.writestr(f"{name}.txt", content)
    return buffer.getvalue()


class Response(BytesIO):
    def __init__(self, content):
        super().__init__(content)
        self.headers = {"Content-Length": str(len(content))}


@pytest.fixture
def load_calls(monkeypatch):
    calls = []

    def load(path, connection, **options):
        calls.append((path, connection, options))
        return {"routes": 1}

    monkeypatch.setattr(refresh, "load_gtfs", load)
    monkeypatch.setattr(refresh.time, "sleep", lambda _: None)
    return calls


def http_feed(monkeypatch, content):
    monkeypatch.setattr(refresh, "urlopen", lambda *args, **kwargs: Response(content))


def test_archives_validated_feed_and_emits_summary(tmp_path, monkeypatch, load_calls, capsys):
    content = feed_bytes()
    http_feed(monkeypatch, content)
    monkeypatch.setattr(refresh, "connection_from_env", lambda: {"password": "secret"})
    assert refresh.main(["--archive-dir", str(tmp_path)]) == 0
    summary = json.loads(capsys.readouterr().out)
    archived = tmp_path / hashlib.sha256(content).hexdigest() / "MBTA_GTFS.zip"
    assert archived.read_bytes() == content
    assert summary["status"] == "loaded"
    assert summary["archive_path"] == str(archived)
    assert summary["feed_version"] == "v1"
    assert summary["row_counts"] == {"routes": 1}
    assert load_calls[0][0] == archived
    assert load_calls[0][2]["skip_unchanged"] is True
    assert not list(tmp_path.glob(".download-*"))


def test_network_retry_cleans_partial_file(tmp_path, monkeypatch, load_calls):
    attempts = []

    class BrokenResponse(Response):
        def read1(self, size):
            if self.tell():
                raise URLError("secret URL must not be printed")
            return super().read1(10)

    def open_response(*args, **kwargs):
        attempts.append(kwargs["timeout"])
        return BrokenResponse(feed_bytes())

    monkeypatch.setattr(refresh, "urlopen", open_response)
    with pytest.raises(refresh.DownloadError, match="after 3 attempts"):
        refresh.refresh_feed(refresh.DEFAULT_URL, tmp_path, {})
    assert attempts == [refresh.DOWNLOAD_TIMEOUT] * 3
    assert not list(tmp_path.iterdir())
    assert not load_calls


def test_transient_http_failure_retries_then_loads(tmp_path, monkeypatch, load_calls):
    attempts = []

    def open_response(*args, **kwargs):
        attempts.append(1)
        if len(attempts) == 1:
            raise HTTPError("private", 503, "unavailable", {}, None)
        return Response(feed_bytes())

    monkeypatch.setattr(refresh, "urlopen", open_response)
    assert refresh.refresh_feed(refresh.DEFAULT_URL, tmp_path, {})["status"] == "loaded"
    assert len(attempts) == 2


@pytest.mark.parametrize("content", [b"not a ZIP", feed_bytes(expired=True)])
def test_invalid_feed_is_not_archived_or_loaded(tmp_path, monkeypatch, load_calls, content):
    http_feed(monkeypatch, content)
    with pytest.raises((ValueError, BadZipFile)):
        refresh.refresh_feed(refresh.DEFAULT_URL, tmp_path, {})
    assert not load_calls
    assert not list(tmp_path.iterdir())


def test_unchanged_and_force_reload(tmp_path, monkeypatch):
    content = feed_bytes()
    http_feed(monkeypatch, content)
    seen = set()

    def load(path, connection, *, skip_unchanged, source_url):
        if path in seen and skip_unchanged:
            return None
        seen.add(path)
        return {"routes": 1}

    monkeypatch.setattr(refresh, "load_gtfs", load)
    assert refresh.refresh_feed(refresh.DEFAULT_URL, tmp_path, {})["status"] == "loaded"
    assert refresh.refresh_feed(refresh.DEFAULT_URL, tmp_path, {})["status"] == "unchanged"
    assert refresh.refresh_feed(refresh.DEFAULT_URL, tmp_path, {}, force=True)["status"] == "loaded"
    assert len(list(tmp_path.iterdir())) == 1


def test_changed_bytes_with_same_version_keep_both_archives(tmp_path, monkeypatch, load_calls):
    summaries = []
    for stop in ("Central", "North"):
        http_feed(monkeypatch, feed_bytes(stop_name=stop))
        summaries.append(refresh.refresh_feed(refresh.DEFAULT_URL, tmp_path, {}))
    assert summaries[0]["feed_version"] == summaries[1]["feed_version"]
    assert summaries[0]["archive_path"] != summaries[1]["archive_path"]
    assert len(list(tmp_path.iterdir())) == len(load_calls) == 2


def test_corrupt_archive_is_not_overwritten(tmp_path, monkeypatch, load_calls):
    content = feed_bytes()
    http_feed(monkeypatch, content)
    directory = tmp_path / hashlib.sha256(content).hexdigest()
    directory.mkdir()
    archived = directory / "MBTA_GTFS.zip"
    archived.write_bytes(b"existing damaged archive")
    with pytest.raises(ValueError, match="checksum"):
        refresh.refresh_feed(refresh.DEFAULT_URL, tmp_path, {})
    assert archived.read_bytes() == b"existing damaged archive"
    assert not load_calls


def test_failure_exit_hides_database_credentials(tmp_path, monkeypatch, capsys):
    http_feed(monkeypatch, feed_bytes())
    monkeypatch.setattr(refresh, "connection_from_env", lambda: {})

    def fail(*args, **kwargs):
        raise refresh.psycopg.OperationalError("password=secret")

    monkeypatch.setattr(refresh, "load_gtfs", fail)
    assert refresh.main(["--archive-dir", str(tmp_path)]) == 1
    error = capsys.readouterr().err
    assert "PostgreSQL" in error
    assert "secret" not in error


def test_permanent_http_failure_is_not_retried(tmp_path, monkeypatch, load_calls):
    attempts = []

    def fail(*args, **kwargs):
        attempts.append(1)
        raise HTTPError("private?token=secret", 404, "missing", {}, None)

    monkeypatch.setattr(refresh, "urlopen", fail)
    with pytest.raises(refresh.DownloadError, match="HTTP 404"):
        refresh.refresh_feed(refresh.DEFAULT_URL, tmp_path, {})
    assert len(attempts) == 1
    assert not load_calls
    assert not list(tmp_path.iterdir())


def test_oversized_download_rejected_before_load(tmp_path, monkeypatch, load_calls):
    monkeypatch.setattr(refresh, "MAX_DOWNLOAD_BYTES", 10)
    http_feed(monkeypatch, feed_bytes())
    with pytest.raises(refresh.DownloadError, match="limit"):
        refresh.refresh_feed(refresh.DEFAULT_URL, tmp_path, {})
    assert not load_calls
    assert not list(tmp_path.iterdir())


def test_source_url_credentials_not_persisted(tmp_path, monkeypatch, load_calls):
    http_feed(monkeypatch, feed_bytes())
    refresh.refresh_feed("https://example.test/feed.zip?token=secret#private", tmp_path, {})
    assert load_calls[0][2]["source_url"] == "https://example.test/feed.zip"


def test_total_timeout_cleans_partial_download(tmp_path, monkeypatch, load_calls):
    http_feed(monkeypatch, feed_bytes())
    times = iter([0, 0, 11])
    monkeypatch.setattr(refresh.time, "monotonic", lambda: next(times))
    with pytest.raises(refresh.DownloadError, match="total time limit"):
        refresh.refresh_feed(refresh.DEFAULT_URL, tmp_path, {}, download_timeout=10)
    assert not list(tmp_path.iterdir())
    assert not load_calls


def test_local_zip_uses_same_archive_without_network(tmp_path, monkeypatch, load_calls, capsys):
    original = tmp_path / "downloaded.zip"
    content = feed_bytes()
    original.write_bytes(content)
    monkeypatch.setattr(refresh, "connection_from_env", lambda: {})

    def fail(*args, **kwargs):
        pytest.fail("Local ZIP mode must not access the network")

    monkeypatch.setattr(refresh, "urlopen", fail)
    assert (
        refresh.main(["--zip-path", str(original), "--archive-dir", str(tmp_path / "archives")])
        == 0
    )
    summary = json.loads(capsys.readouterr().out)
    assert summary["status"] == "loaded"
    assert Path(summary["archive_path"]).read_bytes() == original.read_bytes() == content
    assert load_calls[0][2]["source_url"] is None


def test_local_zip_and_explicit_url_are_mutually_exclusive(tmp_path):
    with pytest.raises(SystemExit) as exit_info:
        refresh.main(["--url", refresh.DEFAULT_URL, "--zip-path", str(tmp_path / "feed.zip")])
    assert exit_info.value.code == 2


@pytest.mark.parametrize("exit_code", [0, 1])
def test_shell_runs_dbt_only_after_successful_refresh(tmp_path, exit_code):
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    wrapper = scripts / "refresh-gtfs.sh"
    wrapper.write_text(
        (Path(__file__).resolve().parents[1] / "scripts/refresh-gtfs.sh").read_text()
    )
    fake_python = scripts / "python"
    fake_python.write_text(f'#!/bin/sh\nprintf "%s\\n" "$@"\nexit {exit_code}\n')
    fake_dbt = scripts / "dbt"
    fake_dbt.write_text('#!/bin/sh\nprintf "DBT:%s\\n" "$@"\n')
    fake_python.chmod(0o755)
    fake_dbt.chmod(0o755)
    result = subprocess.run(
        ["bash", str(wrapper), "--force"],
        cwd=tmp_path,
        env=os.environ | {"GTFS_PYTHON": str(fake_python), "GTFS_DBT": str(fake_dbt)},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == exit_code
    assert "scripts.refresh_gtfs_static\n--force" in result.stdout
    assert ("DBT:build" in result.stdout) == (exit_code == 0)
    if exit_code == 0:
        assert "DBT:path:models/intermediate" in result.stdout

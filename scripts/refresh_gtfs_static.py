"""Download, validate, archive and atomically load the current MBTA GTFS feed."""

import argparse
import csv
import hashlib
import json
import os
import ssl
import sys
import time
from http.client import HTTPException, IncompleteRead
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit, urlunsplit
from urllib.request import Request, urlopen
from zipfile import BadZipFile

import psycopg

from scripts.load_gtfs_static import connection_from_env, inspect_feed, load_gtfs

DEFAULT_URL = "https://cdn.mbta.com/MBTA_GTFS.zip"
DEFAULT_ARCHIVE = Path(__file__).resolve().parents[1] / "data/raw/gtfs_static"
MAX_DOWNLOAD_BYTES = 256 * 1024 * 1024
DOWNLOAD_TIMEOUT = 30
TRANSFER_SECONDS = 900
DOWNLOAD_ATTEMPTS = 3
NETWORK_ERRORS = (URLError, TimeoutError, ConnectionError, HTTPException, ssl.SSLError)


class DownloadError(ValueError):
    """A download failure whose message is safe to display without credentials."""


def _download(url, destination, download_timeout):
    """Retry transient HTTP/transport failures; always restart a partial download."""
    deadline = time.monotonic() + download_timeout
    for attempt in range(DOWNLOAD_ATTEMPTS):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise DownloadError("GTFS download exceeded its total time limit")
        try:
            digest = hashlib.sha256()
            size = 0
            request = Request(url, headers={"User-Agent": "TransitPulse-GTFS-refresh/1.0"})
            with urlopen(request, timeout=min(DOWNLOAD_TIMEOUT, remaining)) as response:
                expected = response.headers.get("Content-Length")
                if expected is not None and int(expected) > MAX_DOWNLOAD_BYTES:
                    raise DownloadError("GTFS download exceeds the 256 MiB limit")
                with destination.open("wb") as output:
                    while chunk := response.read1(64 * 1024):
                        if time.monotonic() > deadline:
                            raise DownloadError("GTFS download exceeded its total time limit")
                        size += len(chunk)
                        if size > MAX_DOWNLOAD_BYTES:
                            raise DownloadError("GTFS download exceeds the 256 MiB limit")
                        digest.update(chunk)
                        output.write(chunk)
                if expected is not None and size != int(expected):
                    raise IncompleteRead(b"", int(expected))
            return digest.hexdigest()
        except HTTPError as exc:
            if exc.code not in (429, 500, 502, 503, 504):
                raise DownloadError(f"GTFS server returned HTTP {exc.code}") from None
        except NETWORK_ERRORS:
            pass
        if attempt + 1 < DOWNLOAD_ATTEMPTS:
            time.sleep(attempt + 1)
    raise DownloadError("GTFS download failed after 3 attempts; check network/server availability")


def _copy_local_feed(source, destination):
    digest = hashlib.sha256()
    size = 0
    with Path(source).open("rb") as input_file, destination.open("wb") as output:
        while chunk := input_file.read(64 * 1024):
            size += len(chunk)
            if size > MAX_DOWNLOAD_BYTES:
                raise ValueError("GTFS ZIP exceeds the 256 MiB limit")
            digest.update(chunk)
            output.write(chunk)
    return digest.hexdigest()


def prepare_feed(url, archive_dir, *, zip_path=None, download_timeout=TRANSFER_SECONDS):
    """Archive validated ZIP bytes under their checksum; never replace an existing feed."""
    if zip_path is None:
        parsed_url = urlsplit(url)
        if parsed_url.scheme not in ("https", "http") or not parsed_url.hostname:
            raise ValueError("GTFS URL must use HTTP or HTTPS")
        if parsed_url.username or parsed_url.password:
            raise ValueError("GTFS URL must not contain embedded credentials")
    if download_timeout <= 0:
        raise ValueError("Download timeout must be greater than zero")
    archive_dir = Path(archive_dir).resolve()
    archive_dir.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix=".download-", dir=archive_dir) as temporary:
        temporary = Path(temporary)
        downloaded = temporary / "MBTA_GTFS.zip"
        checksum = (
            _download(url, downloaded, download_timeout)
            if zip_path is None
            else _copy_local_feed(zip_path, downloaded)
        )
        metadata = inspect_feed(downloaded)
        destination = archive_dir / checksum
        archived = destination / "MBTA_GTFS.zip"
        if not destination.exists():
            try:
                temporary.rename(destination)
            except OSError:
                # Another refresh may have published the same content in the meantime.
                if not archived.is_file():
                    raise
        if not archived.is_file():
            raise ValueError("GTFS archive directory exists without its ZIP file")
        with archived.open("rb") as source:
            if hashlib.file_digest(source, "sha256").hexdigest() != checksum:
                raise ValueError(
                    "Existing GTFS archive failed checksum verification; kept unchanged"
                )
    return archived, metadata, checksum


def refresh_feed(
    url, archive_dir, connection, *, force=False, zip_path=None, download_timeout=TRANSFER_SECONDS
):
    path, metadata, checksum = prepare_feed(
        url, archive_dir, zip_path=zip_path, download_timeout=download_timeout
    )
    source_url = None
    if zip_path is None:
        parsed_url = urlsplit(url)
        source_url = urlunsplit((parsed_url.scheme, parsed_url.netloc, parsed_url.path, "", ""))
    counts = load_gtfs(path, connection, skip_unchanged=not force, source_url=source_url)
    return {
        "status": "unchanged" if counts is None else "loaded",
        "archive_path": str(path),
        "sha256": checksum,
        "feed_version": metadata["feed_version"],
        "feed_start_date": metadata["feed_start_date"],
        "feed_end_date": metadata["feed_end_date"],
        "row_counts": counts,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group()
    source.add_argument(
        "--url", help="HTTP(S) feed URL (default: MBTA_GTFS_STATIC_URL or MBTA CDN)"
    )
    source.add_argument(
        "--zip-path", type=Path, help="Validate/archive an existing ZIP without download"
    )
    parser.add_argument("--archive-dir", type=Path, default=DEFAULT_ARCHIVE)
    parser.add_argument(
        "--download-timeout",
        type=int,
        default=os.getenv("GTFS_DOWNLOAD_TIMEOUT", str(TRANSFER_SECONDS)),
        help="Total download deadline in seconds across retries (default: 900)",
    )
    parser.add_argument(
        "--force", action="store_true", help="Reload even if ZIP checksum is unchanged"
    )
    args = parser.parse_args(argv)
    try:
        summary = refresh_feed(
            args.url or os.getenv("MBTA_GTFS_STATIC_URL", DEFAULT_URL),
            args.archive_dir,
            connection_from_env(),
            force=args.force,
            zip_path=args.zip_path,
            download_timeout=args.download_timeout,
        )
    except (ValueError, BadZipFile) as exc:
        print(f"GTFS refresh failed: {exc}", file=sys.stderr)
        return 1
    except (csv.Error, RuntimeError, NotImplementedError):
        print("GTFS refresh failed: invalid or unsupported ZIP/CSV content", file=sys.stderr)
        return 1
    except psycopg.Error:
        print("GTFS refresh failed: PostgreSQL connection or import error", file=sys.stderr)
        return 1
    except OSError:
        print(
            "GTFS refresh failed: could not read/write the input ZIP or local archive",
            file=sys.stderr,
        )
        return 1
    print(json.dumps(summary, default=str, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())

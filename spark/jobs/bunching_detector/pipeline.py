"""Bounded, driver-local snapshot state for the local console deployment.

SQLite transactions couple snapshot/confirmation changes with a replayable console outbox.
The file belongs to ONE Spark checkpoint and must live on a persistent local volume.
"""

import json
import sqlite3
from collections.abc import Callable, Iterator
from contextlib import closing
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming.query import StreamingQuery
from pyspark.sql.types import StructField, TimestampType

from spark.common.transforms.event_time import add_event_time
from spark.common.transforms.parsing import parse_trip_update_events
from spark.common.validation.data_quality import split_trip_update, trip_update_quality
from spark.jobs.bunching_detector.alert_builder import RULE_VERSION, build_bunching_alerts
from spark.jobs.bunching_detector.detector import detect_bunching_candidates
from spark.jobs.route_reliability.transform import transform_trip_update


@dataclass(frozen=True)
class BunchingSettings:
    headway_threshold_seconds: int = 120
    confirmation_observations: int = 2
    cooldown_seconds: int = 600
    snapshot_lateness_seconds: int = 180
    max_prediction_age_seconds: int = 180
    max_observation_gap_seconds: int = 180
    max_events_per_batch: int = 10000
    max_buffered_events: int = 50000
    route_ids: tuple[str, ...] = ("1", "66", "77")

    def __post_init__(self) -> None:
        if any(
            type(value) is not int or value <= 0
            for name, value in asdict(self).items()
            if name != "route_ids"
        ):
            raise ValueError("Bunching settings must be positive integers")
        if not self.route_ids or any(
            not isinstance(route, str) or not route.strip() for route in self.route_ids
        ):
            raise ValueError("Select at least one non-empty bus route ID")


def prepare_trip_updates(raw_df: DataFrame, settings: BunchingSettings) -> DataFrame:
    valid, _invalid = split_trip_update(trip_update_quality(parse_trip_update_events(raw_df)))
    valid = valid.filter(F.col("route_id").isin(list(settings.route_ids))).filter(
        F.col("feed_timestamp").cast("long") <= F.col("ingested_at").cast("long") + 60
    )
    # Snapshot identity must survive dedup: the same trip event can occur in a NEW feed.
    # Persistent dedup/latest-per-trip is performed by the snapshot store below.
    return add_event_time(
        valid.drop("raw_value", "kafka_key"),
        "feed_timestamp",
        f"{settings.snapshot_lateness_seconds} seconds",
    )


def print_alerts(alerts: list[str]) -> None:
    print(f"Bunching alerts: {len(alerts)} (showing at most 20)", flush=True)
    for alert in alerts[:20]:
        print(alert, flush=True)


def _seconds(value: str) -> float:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def _json_strings(df: DataFrame) -> Iterator[str]:
    encoded = df.select(
        F.to_json(F.struct(*df.columns), {"ignoreNullFields": "false"}).alias("json")
    )
    for row in encoded.toLocalIterator():
        yield str(row.json)


class BunchingProcessor:
    def __init__(
        self,
        checkpoint: Path,
        settings: BunchingSettings,
        emit: Callable[[list[str]], None] = print_alerts,
    ) -> None:
        checkpoint = Path(checkpoint)
        checkpoint.mkdir(parents=True, exist_ok=True)
        self.path = checkpoint / "bunching.sqlite"
        commits = checkpoint / "commits"
        if not self.path.exists() and commits.exists() and any(commits.iterdir()):
            raise ValueError("Missing bunching state for existing Spark checkpoint")
        self.settings = settings
        self.emit = emit

    def __call__(self, batch_df: DataFrame, batch_id: int) -> None:
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("CREATE TABLE IF NOT EXISTS meta (id INTEGER PRIMARY KEY, value TEXT)")
            db.execute(
                "CREATE TABLE IF NOT EXISTS snapshots "
                "(feed REAL, trip TEXT, version REAL, body TEXT, PRIMARY KEY(feed, trip))"
            )
            db.execute("CREATE TABLE IF NOT EXISTS sightings (key TEXT PRIMARY KEY, body TEXT)")
            db.execute("BEGIN IMMEDIATE")
            saved = db.execute("SELECT value FROM meta WHERE id=1").fetchone()
            signature = json.loads(
                json.dumps(
                    {
                        **asdict(self.settings),
                        "rule_version": RULE_VERSION,
                        "schema": batch_df.schema.json(),
                    }
                )
            )
            state = (
                json.loads(saved[0])
                if saved
                else {
                    "batch_id": -1,
                    "max_feed": 0,
                    "closed_through": 0,
                    "previous_snapshot": None,
                    "alerts": [],
                    "signature": signature,
                }
            )
            if state["signature"] != signature:
                raise ValueError("Configuration/schema changed: use a new checkpoint")
            if batch_id < state["batch_id"] or (not saved and batch_id != 0):
                raise ValueError("Batch ID does not match persisted checkpoint state")
            if batch_id == state["batch_id"]:
                alerts = state["alerts"]
            else:
                alerts = self._process(db, state, batch_df)
                state.update(batch_id=batch_id, alerts=alerts)
                db.execute("INSERT OR REPLACE INTO meta VALUES (1, ?)", (json.dumps(state),))
        # Commit first: if console fails, Spark retries and replays exactly this outbox.
        print(f"Batch: {batch_id}", flush=True)
        self.emit(alerts)

    def _process(
        self, db: sqlite3.Connection, state: dict[str, Any], batch_df: DataFrame
    ) -> list[str]:
        late = 0
        for count, body in enumerate(_json_strings(batch_df), start=1):
            if count > self.settings.max_events_per_batch:
                raise ValueError("Per-batch event limit exceeded; lower maxOffsetsPerTrigger")
            event = json.loads(body)
            feed = _seconds(event["feed_timestamp"])
            if feed <= state["closed_through"]:
                late += 1
                continue
            state["max_feed"] = max(state["max_feed"], feed)
            db.execute(
                "INSERT INTO snapshots VALUES (?, ?, ?, ?) ON CONFLICT(feed, trip) DO UPDATE "
                "SET version=excluded.version, body=excluded.body "
                "WHERE excluded.version > snapshots.version OR "
                "(excluded.version = snapshots.version AND excluded.body > snapshots.body)",
                (feed, event["trip_id"], _seconds(event["source_timestamp"]), body),
            )
        cutoff = state["max_feed"] - self.settings.snapshot_lateness_seconds
        feeds = db.execute(
            "SELECT DISTINCT feed FROM snapshots WHERE feed <= ? ORDER BY feed", (cutoff,)
        ).fetchall()
        alerts = []
        for (feed,) in feeds:
            bodies = [
                row[0] for row in db.execute("SELECT body FROM snapshots WHERE feed=?", (feed,))
            ]
            snapshot = (
                batch_df.sparkSession.createDataFrame([(body,) for body in bodies], "json string")
                .select(F.from_json("json", batch_df.schema).alias("event"))
                .select("event.*")
            )
            fresh = snapshot.filter(
                (
                    F.col("feed_timestamp").cast("long") - F.col("source_timestamp").cast("long")
                ).between(-60, self.settings.max_prediction_age_seconds)
            )
            candidates = detect_bunching_candidates(
                transform_trip_update(fresh), self.settings.headway_threshold_seconds
            )
            alerts.extend(self._confirm(db, state, candidates, feed))
            state["previous_snapshot"] = feed
            db.execute("DELETE FROM snapshots WHERE feed=?", (feed,))
        state["closed_through"] = max(state["closed_through"], cutoff)
        buffered = db.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
        if buffered > self.settings.max_buffered_events:
            raise ValueError("Snapshot buffer limit exceeded")
        expiry = state["max_feed"] - (
            self.settings.snapshot_lateness_seconds
            + max(self.settings.cooldown_seconds, self.settings.max_observation_gap_seconds)
        )
        db.execute("DELETE FROM sightings WHERE json_extract(body, '$.last') < ?", (expiry,))
        print(f"Snapshot buffer: {buffered} events; late dropped: {late}", flush=True)
        return alerts

    def _confirm(
        self, db: sqlite3.Connection, state: dict[str, Any], candidates: DataFrame, feed: float
    ) -> list[str]:
        confirmed = []
        for count, row in enumerate(candidates.toLocalIterator(), start=1):
            if count > self.settings.max_buffered_events:
                raise ValueError("Candidate buffer limit exceeded")
            item = row.asDict()
            key = json.dumps(
                [
                    item[name]
                    for name in (
                        "route_id",
                        "direction_id",
                        "stop_id",
                        "leading_trip_id",
                        "following_trip_id",
                    )
                ]
            )
            saved = db.execute("SELECT body FROM sightings WHERE key=?", (key,)).fetchone()
            old: dict[str, Any] = json.loads(saved[0]) if saved else {}
            consecutive = (
                bool(old)
                and old["last"] == state["previous_snapshot"]
                and feed - old["last"] <= self.settings.max_observation_gap_seconds
            )
            current: dict[str, Any] = {
                "count": old["count"] + 1 if consecutive else 1,
                "first": old["first"] if consecutive else feed,
                "last": feed,
                "emitted": old["emitted"] if old else None,
            }
            if current["count"] >= self.settings.confirmation_observations and (
                current["emitted"] is None
                or feed - current["emitted"] >= self.settings.cooldown_seconds
            ):
                item.update(
                    first_detected_at=datetime.fromtimestamp(current["first"], UTC),
                    last_detected_at=datetime.fromtimestamp(feed, UTC),
                )
                confirmed.append(item)
                current["emitted"] = feed
            db.execute("INSERT OR REPLACE INTO sightings VALUES (?, ?)", (key, json.dumps(current)))
        if not confirmed:
            return []
        schema = type(candidates.schema)(
            [
                *candidates.schema.fields,
                StructField("first_detected_at", TimestampType()),
                StructField("last_detected_at", TimestampType()),
            ]
        )
        confirmed_df = candidates.sparkSession.createDataFrame(confirmed, schema)
        return list(
            _json_strings(
                build_bunching_alerts(confirmed_df, self.settings.headway_threshold_seconds)
            )
        )


def start_bunching_query(
    spark: SparkSession,
    raw_df: DataFrame,
    checkpoint: Path,
    settings: BunchingSettings,
    trigger_interval: str = "10 seconds",
    emit: Callable[[list[str]], None] = print_alerts,
) -> StreamingQuery:
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    processor = BunchingProcessor(checkpoint, settings, emit)
    return (
        prepare_trip_updates(raw_df, settings)
        .writeStream.queryName("transitpulse-trip-update-bunching")
        .option("checkpointLocation", str(checkpoint))
        .trigger(processingTime=trigger_interval)
        .foreachBatch(processor)
        .start()
    )

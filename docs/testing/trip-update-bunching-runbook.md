# Single-job TripUpdate and bunching console flow

Run **one** entry point: `spark/jobs/route_reliability/main.py` (recommended), or the
compatibility wrapper `spark/jobs/bunching_detector/main.py`. They launch the same query.
No processed Kafka topic is required. VehiclePosition and ServiceAlert jobs are unchanged.

## Processing contract

1. Read `transit.trip_updates.v1`, parse and apply TripUpdate quality rules once.
2. Restrict to the configured bus route allowlist and reject implausibly future feed timestamps.
3. Attach event-time watermark metadata on `feed_timestamp`. The SQLite store explicitly
   enforces the lateness policy; `withWatermark` alone does not filter or buffer rows.
4. Persist pending snapshots keyed by `(feed_timestamp, trip_id)`. Keep the latest source
   timestamp for a trip; duplicates do not count as additional observations. The same event
   in a NEW snapshot must remain present, so the generic event-ID dedup is not used here.
5. Once newer feed timestamps advance beyond the configured lateness bound, close snapshots
   in chronological order. Restore their typed rows, flatten using the existing TripUpdate
   transform, and compute headway in Spark. Only fresh predictions and each trip's next visit
   at a stop are eligible. Never pair opposite directions, different stops or different routes.
6. Require consecutive observed snapshots for the same ordered trip pair/stop. Missing
   candidates or observation gaps reset confirmation. Use event time for cooldown and replay.
7. Format confirmed observations using `operational_alert_v1.json`. Preserve first/last
   detection timestamps, deterministic identity, vehicle IDs and evidence.
8. Commit state and the batch's console outbox in one SQLite transaction, then print at most
   20 alert JSON records plus the total count. A retried batch reprints the same records/IDs
   without increasing confirmation. Console delivery is at-least-once, not exactly-once.

## Local Docker smoke test

Run commands in `/Users/thanhdanh/transitpulse`.

```bash
docker compose build spark-master spark-worker
docker compose up -d kafka kafka-init spark-master spark-worker
```

Run the query in the foreground (terminal 1):

```bash
docker compose exec \
  -e KAFKA_STARTING_OFFSETS=latest \
  -e TRIP_UPDATE_CHECKPOINT_LOCATION=/opt/spark/checkpoints/trip-update-bunching-v1 \
  --workdir /opt/transitpulse \
  spark-master /opt/spark/bin/spark-submit \
  --conf spark.jars.packages=org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 \
  --conf spark.jars.excludes=org.apache.hadoop:hadoop-client-api,org.apache.hadoop:hadoop-client-runtime,org.xerial.snappy:snappy-java \
  spark/jobs/route_reliability/main.py
```

Start the producer in terminal 2 after the query starts:

```bash
docker compose --profile apps up -d producer
docker compose logs -f producer
```

The YAML dependency is now included in the Spark image. `configs/thresholds.yaml` contains
the bunching threshold, confirmation count, cooldown, lateness and buffer limits.
`configs/routes.example.yaml` selects bus routes (default 1, 66, 77). Override their locations
with `THRESHOLDS_CONFIG_PATH` and `ROUTES_CONFIG_PATH` inside the container if needed.
Set `TRIP_UPDATE_TRIGGER_INTERVAL` to override the default 10-second trigger.

Expect approximately the configured 180-second lateness delay plus confirmation observations
before a real alert is possible. `Bunching alerts: 0` is normal; real traffic need not bunch.
`Snapshot buffer` and `late dropped` logs distinguish pending data from final output.

Stop the foreground query with Ctrl+C; stop the producer with `docker compose stop producer`.
Keep the checkpoint to resume. Do not run both entry points, or share one checkpoint between jobs.
Use a NEW checkpoint when changing rule/configuration/schema; replay then depends on Kafka
retention and `startingOffsets` (ignored on normal checkpoint resume).

## Deterministic local verification without Kafka or Maven downloads

```bash
PYSPARK_PYTHON=/Users/thanhdanh/transitpulse/.venv/bin/python \
PYSPARK_DRIVER_PYTHON=/Users/thanhdanh/transitpulse/.venv/bin/python \
.venv/bin/python -m pytest spark/tests/streaming/test_bunching_stream.py -s -q -o addopts=''
```

This runs a real Structured Streaming query over fixture files shaped like Kafka rows,
splits a snapshot between batches, restarts with the same checkpoint and prints a confirmed
90-second headway alert. It verifies processing and console output, not network connectivity
to Kafka or the MBTA API.

## Explicit deployment limits

- SQLite is a **local, single-driver backend** on the persistent checkpoint volume. It is
  not a distributed Spark state store and does not work on S3 paths. A cloud deployment
  needs a distributed/durable state implementation and migration plan before scaling out.
- Keep `bunching.sqlite` and Spark offsets/commits together. Missing business state fails
  fast rather than silently forgetting confirmations. Do not delete either half separately.
- Pending input and candidate counts have safety caps. Exceeding a cap fails the transaction
  and query for tuning; it never silently truncates candidates to the console's 20 rows.
- The feed has no end-of-snapshot marker. Completeness is bounded by event-time lateness,
  not guaranteed for arbitrarily late rows. Late arrivals after closure are logged and dropped.
  When input stops, the newest snapshots remain pending until feed time advances on resume.
- These are heuristic **predicted** bunching warnings. A fixed 120-second threshold is not
  schedule-normalized and can flag intentionally close trips. GTFS static headway enrichment
  is a future refinement; this job does not claim observed physical bunching.
- This flow emits `active` warnings only. Missing/stale telemetry resets confirmation or expires
  internal history; it is not proof of recovery, so no false `resolved` event is emitted.
  Automatic incident resolution, database sinks and a durable DLQ are not part of this console flow.
- Invalid TripUpdate rows are filtered before the snapshot store; they are not yet sent to DLQ.

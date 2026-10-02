# TransitPulse dashboard and operations guide

## Purpose

The dashboard is the presentation and operations layer for the TransitPulse pipeline:

```text
MBTA GTFS Realtime -> Kafka -> Spark Structured Streaming -> PostgreSQL staging
                    -> dbt marts -> Streamlit dashboard
```

It separates three different timestamps so the UI never implies that a mart changes every
time Streamlit reruns:

- **Event observed**: the newest realtime event stored by Spark.
- **Mart build**: the newest successful Airflow-managed dbt build.
- **Dashboard read**: when Streamlit most recently queried PostgreSQL.

User-facing timestamps are rendered in `America/New_York`, while PostgreSQL and pipeline
metadata remain in UTC.

## Dashboard pages

### Overview

`dashboard/pages/1_overview.py` is the default landing page. It shows:

- fresh MBTA bus vehicles only (`route_type = 3`);
- active service alerts from the latest known non-empty snapshot;
- predicted-late rate and P90 predicted lateness;
- observed and eligible sample counts plus coverage;
- a rate-and-volume trend with visible gaps;
- route-directions needing attention after a configurable minimum sample threshold.

The route ranking query reads the sample-grain fact table. A route with a high rate based
on only a few observations is excluded until it meets the selected minimum.

### Route reliability

`dashboard/pages/2_route_reliability.py` supports time, route, direction, minimum-sample,
and trip-sample sorting filters. It contains:

- current-window KPIs;
- deltas against the immediately preceding window of equal length;
- predicted-late rate and eligible-volume trend;
- route comparison;
- observation-level trip drill-down.

Rows in the drill-down are **observation samples**, not unique completed trips. The same
trip can appear in multiple five-minute buckets.

### Live operations

`dashboard/pages/3_live_operations.py` provides:

- a map of fresh bus positions with valid coordinates;
- route, direction, and vehicle search controls;
- persistent vehicle and alert selection across Streamlit reruns;
- vehicle route, trip, headsign, occupancy, observation time, and event age;
- alert effect/severity filtering and an explicit route-scope description.

Old positions are hidden rather than presented as live. Alerts without a route ID remain
visible and are labelled as having no explicit route scope.

### Pipeline health

`dashboard/pages/4_pipeline_health.py` combines three independently sourced signals:

- Spark progress persisted from `StreamingQueryListener` callbacks;
- Kafka broker low/high watermarks;
- Airflow-managed dbt run metadata.

Container state alone is not treated as pipeline health evidence. Producer status is
therefore `Unknown` until the producer emits an independent heartbeat.

## Data grains and metric definitions

### `mart.fct_trip_monitoring_samples`

Grain: one trip stop-update observation at one observation time. Its practical identity
includes the event, stop-update index, trip instance, and observation timestamp.

This is the source for whole-window P90 and route-attention calculations.

### `mart.route_health_5m`

Grain: one five-minute observation bucket, route, and direction. It is used for trend and
route-comparison displays.

### `mart.vehicle_latest_state`

Grain: one latest known row per vehicle. `is_fresh` and `source_age_seconds` distinguish a
usable live position from historical state.

### `mart.service_alerts_latest_snapshot`

Grain: one alert from the latest stored non-empty snapshot, with route/stop/trip scopes
collected into arrays.

### Predicted-late rate

```text
late eligible samples / all eligible samples
```

The dashboard sums numerators and denominators before division. It never averages rates
that were already calculated for individual buckets.

`predicted late` means the predicted arrival/departure is more than five minutes after the
GTFS schedule. It is not a confirmed actual arrival delay.

### Coverage

```text
eligible samples / observed samples
```

An eligible sample has a calculable predicted schedule delay. Zero denominators and absent
comparison periods display as unavailable rather than zero.

### P90 predicted lateness

P90 is calculated with `PERCENTILE_CONT(0.9)` over eligible sample-level delay values for
the selected window. Negative delays are clamped to zero for the lateness distribution.
Bucket-level P90 values are not averaged or added.

### Kafka backlog

For each streaming topic and partition:

```text
broker high watermark - end offset of the latest successful Spark progress event
```

Spark manages offsets through checkpoints, so a regular Kafka consumer-group lag is not
used as a substitute. When Spark has not reported an offset, backlog remains unavailable.

### DLQ count

The displayed DLQ count is the number of records currently retained by the broker:

```text
sum(partition high watermark - partition low watermark)
```

It is not a historical total and not a count created during the current UI window.

## Pipeline metrics

Migration `database/migrations/007_create_pipeline_metrics.sql` creates:

- `ops.spark_application_status`;
- `ops.spark_streaming_progress`;
- `ops.dbt_run_history`.

`spark/common/metrics/streaming_listener.py` records query lifecycle and micro-batch
progress from the Spark driver. It stores batch ID, input rows, input/processing rates,
batch duration, and Kafka source end offsets. Metrics failures are logged but never stop
the data query. Progress retention is bounded to the newest 500 rows per application.

The three job entry points register the listener before starting their query and remove it
during normal shutdown.

Spark statuses shown by the dashboard are:

- **Running**: a recent successful progress event exists;
- **Idle**: Spark emitted a recent idle event;
- **Stale**: the last progress event is older than the health threshold;
- **Stopped**: a normal termination event was recorded;
- **Failed**: Spark reported a query exception;
- **Unknown**: there is not enough evidence.

## Airflow dbt refresh

The Docker Airflow deployment uses Airflow 3 components:

- API server;
- scheduler with `LocalExecutor`;
- standalone DAG processor.

The shared configuration supplies an internal Execution API URL and JWT secret so task
processes can report state to the API server.

`airflow/dags/refresh_dashboard_marts.py` schedules a finite dbt build. It does not use an
Airflow task to own long-running Spark applications.

Defaults and safety controls:

- schedule: `*/5 * * * *`, configurable with `DBT_REFRESH_SCHEDULE`;
- `max_active_runs=1` prevents overlapping builds;
- one retry after one minute;
- ten-minute task execution timeout;
- eight-minute dbt subprocess timeout by default;
- fifteen-minute DAG run timeout;
- credentials are passed through environment variables;
- the command is an argument list, not interpolated shell text.

The selector builds dashboard marts, their ancestors, and the alert-period intermediate
model needed by selected data tests. `ops.dbt_run_history` records the runtime status.
Abandoned `running` metadata older than twenty minutes is displayed as stale.

## Local startup

Load the project environment first:

```bash
set -a
source .env
set +a
```

Start the database and Kafka:

```bash
docker compose up -d postgres kafka kafka-init
```

Apply migration 007 to an existing PostgreSQL volume:

```bash
docker compose exec -T postgres \
  sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1' \
  < database/migrations/007_create_pipeline_metrics.sql
```

Initialize and start Airflow:

Set `AIRFLOW_API_SECRET_KEY` and `AIRFLOW_API_AUTH_JWT_SECRET` to two different long,
random values in the local `.env` first. The repository does not provide credential
fallbacks for these settings.

```bash
docker compose --profile orchestration up airflow-init
docker compose --profile orchestration up -d \
  airflow-api-server airflow-scheduler airflow-dag-processor
```

Start the dashboard without starting the producer:

```bash
docker compose --profile apps up -d dashboard
```

Open:

- Dashboard: <http://localhost:8501>
- Airflow: <http://localhost:8083>

Airflow 3 Simple Auth generates the local admin password when the API server starts. Read
it from the local API-server logs rather than storing it in the repository:

```bash
docker compose --profile orchestration logs airflow-api-server
```

Run a manual finite refresh:

```bash
docker compose --profile orchestration exec airflow-scheduler \
  airflow dags trigger refresh_transitpulse_dashboard_marts
```

Inspect import errors and recent runs:

```bash
docker compose --profile orchestration exec airflow-scheduler \
  airflow dags list-import-errors
docker compose --profile orchestration exec airflow-scheduler \
  airflow dags list-runs refresh_transitpulse_dashboard_marts --no-backfill
```

## Verification commands

```bash
.venv/bin/python -m pytest dashboard/tests -q
.venv/bin/python -m pytest airflow/tests/test_dbt_refresh.py -q
docker compose run --rm --no-deps --entrypoint python3 \
  spark-master -m pytest spark/tests/unit -q
.venv/bin/ruff check dashboard spark airflow
.venv/bin/dbt parse --project-dir dbt --profiles-dir dbt
.venv/bin/dbt build --project-dir dbt --profiles-dir dbt \
  --select +vehicle_latest_state +route_health_5m \
  +service_alerts_latest_snapshot +int_service_alert_periods
docker compose --profile orchestration --profile apps config --quiet
git diff --check
```

## Current limitations

1. The producer has no durable heartbeat, so its status cannot be proven from Kafka or
   Spark activity.
2. Empty GTFS realtime alert snapshots create no entity rows. The current mart therefore
   cannot prove that a newer empty snapshot superseded the newest non-empty snapshot.
3. Metrics use local PostgreSQL tables and a fixed row-retention policy. A production
   deployment would normally export them to a monitoring system with time-based retention.
4. Force-killing a Spark container can prevent its termination callback from being stored;
   the health page safely degrades the last state to `Stale` when progress stops.
5. Airflow Simple Auth is suitable for this local deployment only. Production deployments
   should load the required secrets from a secret manager and use a hardened authentication
   manager.

## Main files changed

- `.streamlit/config.toml`: shared supported Streamlit theme.
- `dashboard/app.py`: default Overview navigation and four-page information architecture.
- `dashboard/components/`: shared filters, metric formatting, charts, and vehicle map.
- `dashboard/queries/`: parameterized analytical and operational queries.
- `dashboard/pages/`: Overview, Route Reliability, Live Operations, and Pipeline Health.
- `database/migrations/007_create_pipeline_metrics.sql`: bounded operational metadata.
- `spark/common/metrics/streaming_listener.py`: real Spark progress collection.
- `spark/jobs/*/main.py`: metrics listener registration.
- `airflow/`: Docker image, Airflow 3 DAG, dbt profile, and run metadata helper.
- `docker-compose.yml`: Airflow 3 services, dashboard Kafka access, and migration mounting.
- `dashboard/tests/`, `spark/tests/unit/`, `airflow/tests/`: regression coverage.

# TransitPulse

TransitPulse is a portfolio-grade public-transit streaming platform built around Apache Kafka and Spark Structured Streaming. It ingests MBTA GTFS Realtime feeds, computes operational metrics and alerts, stores serving data in PostgreSQL, and presents results through Streamlit.

The Kafka producer, three Spark-to-PostgreSQL flows, dbt marts, and three Streamlit
dashboard views are implemented. Orchestration remains a subsequent project stage.

## Data sources

- Vehicle positions: `https://cdn.mbta.com/realtime/VehiclePositions.pb`
- Trip updates: `https://cdn.mbta.com/realtime/TripUpdates.pb`
- Service alerts: `https://cdn.mbta.com/realtime/Alerts.pb`
- GTFS Static: `https://cdn.mbta.com/MBTA_GTFS.zip`
- Historical data: `https://performancedata.mbta.com/`

MBTA/MassDOT remains the provider of the source data. Review and follow the current source-data license and attribution requirements before publishing derived datasets.

## Prerequisites

- Docker Desktop or Docker Engine with Compose v2
- At least 8 GB RAM available to Docker; 12 GB is recommended when Airflow is enabled
- Python 3.12 for host-side development
- GNU Make is optional but recommended

## Initial setup

1. Copy `.env.example` to `.env` and change all local-development passwords.
2. Review `configs/routes.example.yaml` and select 3–5 MBTA routes.
3. Run `make bootstrap`.
4. Run `make up` to start Kafka, Spark, PostgreSQL, and topic initialization.
5. Run `make tools-up` if Kafka UI is needed.
6. Run `make smoke` to verify the infrastructure.

The `streaming` profile starts the producer and all three Spark jobs. The `apps` profile
includes the dashboard and is not needed to collect streaming data.

## Dashboard

Start PostgreSQL and the Streamlit app with `docker compose up -d --build dashboard`,
then open `http://localhost:8501`. The app has Overview, Route Reliability, and Live
Operations pages. Live Operations maps only bus positions marked fresh by the dbt
view (currently 90 seconds), and shows service alerts only while their stored feed
snapshot is fresh (currently 10 minutes). It displays the last observed timestamps
so an empty selection is not mistaken for a stopped feed.

The alert mart represents the latest stored **non-empty** snapshot; newer empty
source snapshots are not persisted yet. The page labels this limitation instead of
claiming that a displayed zero is a confirmed absence of source alerts. Live
Operations reads dbt views, while the Route Reliability table marts need a new
`dbt build` after incoming Spark data to reflect recent trip updates.

## Load GTFS Static reference data

With PostgreSQL running, the GTFS/dbt Python extras installed, and `.env` plus
`dbt/profiles.yml` configured, refresh reference data and build/test the dbt views:

```bash
./scripts/refresh-gtfs.sh
```

See [GTFS Static refresh workflow](docs/gtfs-static-refresh.md) for setup, safe
replacement, replay, and verification. The [COPY import guide](docs/testing/gtfs-static-import.md)
documents the lower-level loader. Kafka and Spark are not needed for this batch job.

## Run all three streaming flows

Set `SPARK_WORKER_CORES=3` and `SPARK_WORKER_MEMORY=4g` in `.env`, then run:

```bash
docker compose build spark-master spark-worker
docker compose --profile streaming up -d
docker compose logs -f --tail 50 spark-job-vehicle spark-job-trip spark-job-alert
```

Each job has its own driver container and checkpoint, and requests at most one executor core.
See [parallel Spark jobs](docs/testing/parallel-spark-jobs.md) for resource sizing,
restart commands, and verification results.

## Service endpoints

| Service | Local endpoint | Profile |
|---|---|---|
| Kafka external listener | `localhost:29092` | default |
| Spark master UI | `http://localhost:8081` | default |
| Spark worker UI | `http://localhost:8082` | default |
| PostgreSQL | `localhost:5432` | default |
| Kafka UI | `http://localhost:8080` | `tools` |
| Airflow API/UI | `http://localhost:8083` | `orchestration` |
| Streamlit | `http://localhost:8501` | `apps` |

## Common commands

- `make up`: start core infrastructure
- `make down`: stop services without deleting volumes
- `make tools-up`: start Kafka UI
- `make airflow-up`: initialize and start Airflow
- `make ps`: show service state
- `make logs`: follow core-service logs
- `make topics`: reconcile Kafka topics
- `make smoke`: run infrastructure checks
- `make lint`: run static checks after source code is added
- `make test`: run tests when test files exist

## Configuration

- `.env`: runtime secrets and image/version overrides; never commit it
- `configs/app.example.yaml`: source, Kafka, Spark, PostgreSQL, and runtime defaults
- `configs/routes.example.yaml`: route allowlist
- `configs/thresholds.yaml`: business and data-quality thresholds
- `configs/logging.yaml`: structured logging policy
- `spark/conf/spark-defaults.conf`: Spark Kafka/PostgreSQL packages and streaming defaults

## Implementation order

1. Validate and record GTFS Realtime snapshots.
2. Implement the Kafka producer and contracts.
3. Implement Spark event-time parsing, watermarking, and deduplication.
4. Add current vehicle state and route-window metrics.
5. Add bunching/stale-update detection.
6. Connect PostgreSQL serving tables and Streamlit.
7. Add Airflow and dbt analytical workflows.
8. Add replay, resilience, and performance evidence.

See the project blueprint in the parent workspace for detailed requirements and Definition of Done.

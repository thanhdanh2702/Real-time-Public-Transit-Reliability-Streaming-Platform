# TransitPulse

TransitPulse is a portfolio-grade public-transit streaming platform built around Apache Kafka and Spark Structured Streaming. It ingests MBTA GTFS Realtime feeds, computes operational metrics and alerts, stores serving data in PostgreSQL, and presents results through Streamlit.

This repository is currently a configured scaffold. Business-logic files are intentionally empty so implementation can follow the project blueprint step by step.

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

The `apps` profile is intentionally not started by default because producer, Spark jobs, and dashboard application files are empty in this scaffold.

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

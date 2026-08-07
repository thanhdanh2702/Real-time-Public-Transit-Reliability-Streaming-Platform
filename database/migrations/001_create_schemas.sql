BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS serving;
CREATE SCHEMA IF NOT EXISTS mart;
CREATE SCHEMA IF NOT EXISTS ops;

COMMENT ON SCHEMA raw IS 'Source-aligned batch and reference data.';
COMMENT ON SCHEMA staging IS 'Validated and normalized staging relations.';
COMMENT ON SCHEMA core IS 'Historical facts and conformed dimensions.';
COMMENT ON SCHEMA serving IS 'Low-latency tables optimized for the dashboard.';
COMMENT ON SCHEMA mart IS 'Analytical models managed by dbt.';
COMMENT ON SCHEMA ops IS 'Pipeline audit, health, and data-quality metadata.';

COMMIT;

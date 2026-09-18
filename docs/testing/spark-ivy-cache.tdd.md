# Spark Ivy cache TDD evidence

## User journey

As a local developer, I want Spark dependencies to be reused across container recreations
and bundled Hadoop libraries to be excluded from Maven resolution, so that a streaming job
starts without repeatedly downloading large JAR files.

## Evidence

| Guarantee | Validation | Result |
|---|---|---|
| Spark Master mounts a persistent Ivy cache volume | `test_spark_master_persists_ivy_dependency_cache` | PASS |
| Runtime resolution excludes Hadoop client JARs already bundled in the Spark image | `test_spark_package_resolution_excludes_bundled_hadoop_jars` | PASS |
| Compose configuration remains valid | `make config-check` | PASS |
| Spark image builds with a writable cache directory owned by UID 185 | `docker compose build spark-master spark-worker` and runtime `stat` | PASS |
| The streaming application starts without an open Hadoop `.part` download | Spark Master API and JVM file-descriptor inspection | PASS |
| Producer-to-Kafka-to-Spark-to-PostgreSQL flow works | Producer logs, Spark Master API, and PostgreSQL row count | PASS |

## RED/GREEN record

- RED: the persistent-cache test failed because `spark-master` had no Ivy volume.
- GREEN: both infrastructure tests pass after adding the volume and bundled-JAR exclusions.
- Runtime: `transitpulse-vehicle-state` is `RUNNING` with 2 cores and 2048 MB; PostgreSQL
  received vehicle-position rows after the producer restarted.

## Coverage and known gaps

The focused infrastructure tests pass (`2 passed`). The full suite reached 80% coverage but
has 12 pre-existing host-environment failures because the PySpark driver uses Python 3.12
while the locally spawned worker resolves Python 3.14. Those failures are unrelated to the
Docker/Ivy changes and need a separate Python environment fix.

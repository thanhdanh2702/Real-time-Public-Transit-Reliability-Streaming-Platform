from pathlib import Path

import pytest
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_spark_master_persists_ivy_dependency_cache() -> None:
    compose = yaml.safe_load((PROJECT_ROOT / "docker-compose.yml").read_text())

    spark_master_volumes = compose["services"]["spark-master"]["volumes"]

    assert "spark-ivy-cache:/opt/transitpulse/.ivy2" in spark_master_volumes
    assert "spark-ivy-cache" in compose["volumes"]


def test_spark_package_resolution_excludes_bundled_hadoop_jars() -> None:
    settings: dict[str, str] = {}

    for line in (PROJECT_ROOT / "spark/conf/spark-defaults.conf").read_text().splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("#"):
            key, value = stripped.split(maxsplit=1)
            settings[key] = value

    excluded_packages = set(settings["spark.jars.excludes"].split(","))

    assert {
        "org.apache.hadoop:hadoop-client-api",
        "org.apache.hadoop:hadoop-client-runtime",
    } <= excluded_packages


@pytest.mark.parametrize(
    ("service", "entrypoint"),
    [
        ("spark-job-vehicle", "vehicle_state"),
        ("spark-job-trip", "route_reliability"),
        ("spark-job-alert", "service_alert"),
    ],
)
def test_streaming_drivers_are_reachable_and_cannot_monopolize_worker(service, entrypoint):
    compose = yaml.safe_load((PROJECT_ROOT / "docker-compose.yml").read_text())
    job = compose["services"][service]
    command = job["command"]
    assert command[-1] == f"/opt/transitpulse/spark/jobs/{entrypoint}/main.py"
    assert "spark.cores.max=1" in command
    assert "spark.executor.cores=1" in command
    assert "spark.dynamicAllocation.enabled=false" in command
    assert f"spark.driver.host={service}" in command
    assert "spark.driver.bindAddress=0.0.0.0" in command
    assert "transitpulse" in job["networks"]
    assert job["restart"] == "unless-stopped"
    assert job["depends_on"]["kafka-init"]["condition"] == "service_completed_successfully"
    assert job["depends_on"]["spark-worker"]["condition"] == "service_healthy"


def test_streaming_queries_have_distinct_persistent_checkpoints():
    services = yaml.safe_load((PROJECT_ROOT / "docker-compose.yml").read_text())["services"]
    checkpoints = []
    for name in ("spark-job-vehicle", "spark-job-trip", "spark-job-alert"):
        job = services[name]
        assert "spark-checkpoints:/opt/spark/checkpoints" in job["volumes"]
        checkpoints.extend(
            value
            for key, value in job["environment"].items()
            if key.endswith("CHECKPOINT_LOCATION")
        )
    assert len(checkpoints) == len(set(checkpoints)) == 3
    assert "streaming" in services["producer"]["profiles"]

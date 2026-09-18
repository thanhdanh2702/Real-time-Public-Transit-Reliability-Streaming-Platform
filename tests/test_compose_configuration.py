from pathlib import Path

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

import importlib.util

from tests.plugins.floci_managed_kafka import FlociManagedKafkaService


def test_remove_floci_managed_kafka_shim_when_official_provider_is_available() -> "None":
    assert importlib.util.find_spec("pytest_databases.docker.floci_gcp") is None, (
        "pytest-databases now supplies pytest_databases.docker.floci_gcp; delete the temporary Floci providers "
        "and register the upstream plugin instead."
    )


def test_floci_managed_kafka_service_matches_shared_floci_shape() -> "None":
    assert FlociManagedKafkaService.__dataclass_fields__.keys() >= {
        "project_id",
        "resource_prefix",
        "docker_host",
        "capabilities",
    }

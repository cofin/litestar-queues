import importlib.util


def test_remove_floci_gcp_shim_when_official_provider_is_available() -> "None":
    assert importlib.util.find_spec("pytest_databases.docker.floci_gcp") is None, (
        "pytest-databases now supplies pytest_databases.docker.floci_gcp; delete tests.plugins.floci_gcp "
        "and register the upstream plugin instead."
    )


def test_floci_gcp_service_exposes_joined_emulator_contract() -> "None":
    from tests.plugins.floci_gcp import FlociGcpService

    assert FlociGcpService.__dataclass_fields__.keys() >= {
        "project_id",
        "resource_prefix",
        "docker_host",
        "capabilities",
    }

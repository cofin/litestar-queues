import importlib.util


def test_remove_redpanda_shim_when_official_provider_is_available() -> "None":
    assert importlib.util.find_spec("pytest_databases.docker.redpanda") is None, (
        "pytest-databases now supplies pytest_databases.docker.redpanda; delete tests.plugins.redpanda "
        "and register the upstream plugin instead."
    )

import importlib.util


def test_remove_pubsub_shim_when_official_provider_is_available() -> "None":
    assert importlib.util.find_spec("pytest_databases.docker.pubsub") is None, (
        "pytest-databases now supplies pytest_databases.docker.pubsub; delete tests.plugins.pubsub "
        "and register the upstream plugin instead."
    )

import importlib.util


def test_remove_rabbitmq_shim_when_official_provider_is_available() -> "None":
    assert importlib.util.find_spec("pytest_databases.docker.rabbitmq") is None, (
        "pytest-databases now supplies pytest_databases.docker.rabbitmq; delete tests.plugins.rabbitmq "
        "and register the official plugin in tests/conftest.py."
    )

import importlib.util


def test_remove_localstack_shim_when_official_provider_is_available() -> "None":
    assert importlib.util.find_spec("pytest_databases.docker.localstack") is None, (
        "pytest-databases now supplies pytest_databases.docker.localstack; delete tests.plugins.localstack "
        "and register the official plugin in tests/conftest.py."
    )

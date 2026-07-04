from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from litestar import Litestar

    from litestar_queues import QueueConfig, QueuePlugin

# pytest-databases auto-skips each plugin when Docker is unavailable; declared
# at the project root because pytest requires ``pytest_plugins`` to live in the
# top-level conftest.
pytest_plugins = [
    "pytest_databases.docker.cockroachdb",
    "pytest_databases.docker.postgres",
    "pytest_databases.docker.mysql",
    "pytest_databases.docker.mssql",
    "pytest_databases.docker.oracle",
    "pytest_databases.docker.mssql",
    "pytest_databases.docker.spanner",
    "pytest_databases.docker.redis",
    "pytest_databases.docker.valkey",
]

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend() -> "str":
    """Return the async backend to use for tests."""
    return "asyncio"


@pytest.fixture(autouse=True)
def clean_task_registry() -> "None":
    """Clear queue task registries before each test."""
    from litestar_queues.task import clear_task_registry

    clear_task_registry()


@pytest.fixture
def queue_config() -> "QueueConfig":
    """Return a default queue configuration for testing."""
    from litestar_queues import QueueConfig

    return QueueConfig(queue_backend="memory", in_app_worker=False)


@pytest.fixture
def queue_plugin(queue_config: "QueueConfig") -> "QueuePlugin":
    """Return a queue plugin instance for testing."""
    from litestar_queues import QueuePlugin

    return QueuePlugin(config=queue_config)


@pytest.fixture
def app(queue_plugin: "QueuePlugin") -> "Litestar":
    """Return a Litestar application with the queue plugin."""
    from litestar import Litestar

    return Litestar(plugins=[queue_plugin])

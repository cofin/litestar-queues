import logging

import pytest
from litestar import Litestar
from litestar.logging.config import LoggingConfig

from litestar_queues import QueueConfig, QueuePlugin, WorkerConfig
from litestar_queues.task import clear_task_registry

pytestmark = pytest.mark.anyio


def pytest_configure(config: "pytest.Config") -> "None":
    """Disable Litestar's default QueueListener logging before test module collection."""
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(LoggingConfig, "configure", lambda _self: logging.getLogger)
    config.add_cleanup(monkeypatch.undo)


@pytest.fixture
def anyio_backend() -> "str":
    """Return the async backend to use for tests."""
    return "asyncio"


@pytest.fixture(autouse=True)
def clean_task_registry(monkeypatch: "pytest.MonkeyPatch") -> "None":
    """Clear queue task registries and prevent Litestar QueueListener thread leaks across tests."""
    monkeypatch.setattr(LoggingConfig, "configure", lambda _self: logging.getLogger)
    clear_task_registry()


@pytest.fixture
def queue_config() -> "QueueConfig":
    """Return a default queue configuration for testing."""
    return QueueConfig(queue_backend="memory", worker=WorkerConfig(placement="external"))


@pytest.fixture
def queue_plugin(queue_config: "QueueConfig") -> "QueuePlugin":
    """Return a queue plugin instance for testing."""
    return QueuePlugin(config=queue_config)


@pytest.fixture
def app(queue_plugin: "QueuePlugin") -> "Litestar":
    """Return a Litestar application with the queue plugin."""
    return Litestar(plugins=[queue_plugin], logging_config=None)

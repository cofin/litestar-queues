"""Unit-tier pytest fixtures."""

from collections.abc import AsyncIterator

import pytest

from litestar_queues import QueueConfig, QueueService
from litestar_queues.events import (
    InMemoryQueueEventSink,
    NoopQueueEventSink,
    QueueEventSink,
)


@pytest.fixture(params=["in_memory", "noop"], ids=["in_memory", "noop"])
def event_sink(request: pytest.FixtureRequest) -> QueueEventSink:
    """Return a parametrized QueueEventSink over both built-in sinks."""
    if request.param == "in_memory":
        return InMemoryQueueEventSink()
    return NoopQueueEventSink()


@pytest.fixture
async def queue_service_memory() -> AsyncIterator[QueueService]:
    """Yield a lifecycle-managed QueueService backed by memory + local execution."""
    async with QueueService(QueueConfig(queue_backend="memory", execution_backend="local")) as service:
        yield service

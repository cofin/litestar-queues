"""Unit-tier pytest fixtures."""

from typing import TYPE_CHECKING

import pytest

from litestar_queues import QueueConfig, QueueService, WorkerConfig
from litestar_queues.events import InMemoryQueueEventSink, NoopQueueEventSink, QueueEventSink

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


@pytest.fixture(autouse=True)
def _no_real_cloud_tasks_client(monkeypatch: "pytest.MonkeyPatch") -> "None":
    """Turn an un-injected Cloud Tasks client into a failure, not a network call.

    ``google.cloud.tasks_v2.CloudTasksAsyncClient()`` picks up ambient
    application-default credentials, so a unit test that forgets to inject a
    fake does not fail -- it authenticates as whoever is running the suite and
    calls Google. The lazy import is the single place that happens, so it is
    the single place to close.
    """
    from litestar_queues.execution.cloudtasks import backend as cloud_tasks_backend

    def _refuse(module_path: "str") -> "None":
        msg = (
            f"A unit test asked the Cloud Tasks backend to import {module_path!r} and build a real "
            f"client, which would authenticate and call Google. Pass client=FakeCloudTasksClient() to "
            f"CloudTasksExecutionBackend instead."
        )
        raise AssertionError(msg)

    monkeypatch.setattr(cloud_tasks_backend, "import_module", _refuse)


@pytest.fixture(params=["in_memory", "noop"], ids=["in_memory", "noop"])
def event_sink(request: "pytest.FixtureRequest") -> "QueueEventSink":
    """Return a parametrized QueueEventSink over both built-in sinks."""
    if request.param == "in_memory":
        return InMemoryQueueEventSink()
    return NoopQueueEventSink()


@pytest.fixture
async def queue_service_memory() -> "AsyncIterator[QueueService]":
    """Yield a lifecycle-managed QueueService backed by memory + local execution."""
    async with QueueService(
        QueueConfig(worker=WorkerConfig(placement="external"), queue_backend="memory", execution_backend="local")
    ) as service:
        yield service

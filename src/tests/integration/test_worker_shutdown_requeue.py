"""Behavioral shutdown-requeue proof for every registered queue backend."""

from typing import TYPE_CHECKING, Any, cast

import pytest

from tests.integration._interrupt_contract import assert_worker_shutdown_requeues_running_task

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from litestar_queues.backends import BaseQueueBackend
    from litestar_queues.models import QueuedTaskRecord

pytestmark = pytest.mark.anyio


async def test_worker_shutdown_requeues_running_task(queue_backend: "BaseQueueBackend") -> "None":
    await assert_worker_shutdown_requeues_running_task(queue_backend)


async def test_task_dependency_provider_closes_on_shutdown_interruption(queue_backend: "BaseQueueBackend") -> "None":
    import asyncio
    import contextlib

    from litestar_queues import QueueConfig, QueueService, WorkerConfig, task
    from litestar_queues.task import clear_task_registry
    from litestar_queues.worker import Worker

    clear_task_registry()

    events: "list[str]" = []
    started = asyncio.Event()

    from litestar_queues import Task, TaskExecutionContext

    @contextlib.asynccontextmanager
    async def provider(
        _task: "Task[..., object]", _record: "QueuedTaskRecord", _context: "TaskExecutionContext"
    ) -> "AsyncIterator[dict[str, object]]":
        events.append("acquire")
        try:
            yield {"injected_service": "from-provider"}
        except asyncio.CancelledError:
            events.append("cleanup")
            raise
        finally:
            if "cleanup" not in events:
                events.append("cleanup")

    @task("contract.provider.shutdown")
    async def consume(**kwargs: "object") -> "dict[str, object]":
        events.append("body")
        started.set()
        await asyncio.sleep(10)
        return {"injected_service": kwargs["injected_service"]}

    config = QueueConfig(
        worker=WorkerConfig(
            placement="external",
            requeue_on_shutdown=True,
            max_concurrency=1,
            graceful_shutdown_timeout=0.05,
            final_cancel_timeout=1.0,
            poll_interval=0.05,
        ),
        queue_backend="memory",
        execution_backend="local",
        task_dependency_provider=cast("Any", provider),
    )
    service = QueueService(config, queue_backend=queue_backend)

    async with service:
        worker = Worker(service)
        worker_task = asyncio.create_task(worker.start())

        result = await service.enqueue("contract.provider.shutdown")
        await asyncio.wait_for(started.wait(), timeout=5)

        # Give a small moment for DB to update to "running" if not memory
        await asyncio.sleep(0.1)

        await worker.stop()
        await asyncio.wait_for(worker_task, timeout=5)
        record = await queue_backend.get_task(result.id)

    assert events == ["acquire", "body", "cleanup"]
    assert record is not None
    assert record.status == "pending"
    assert record.started_at is None
    assert record.worker_id is None

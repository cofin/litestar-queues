from dataclasses import replace
from typing import TYPE_CHECKING

import pytest

from litestar_queues.backends.memory.backend import InMemoryQueueBackend
from litestar_queues.config import QueueConfig
from litestar_queues.execution.base import BaseExecutionBackend, ExecutionCancelResult
from litestar_queues.execution.cloudrun.backend import CloudRunExecutionBackend
from litestar_queues.service import QueueService

if TYPE_CHECKING:
    from litestar_queues.models import QueuedTaskRecord


@pytest.mark.anyio
async def test_execution_cancel_result_constructors_and_durable_predicate() -> "None":
    accepted = ExecutionCancelResult.accepted()
    assert accepted.status == "accepted"
    assert accepted.detail is None
    assert accepted.permits_durable_cancel is True

    already_cancelled = ExecutionCancelResult.already_cancelled()
    assert already_cancelled.status == "already_cancelled"
    assert already_cancelled.permits_durable_cancel is True

    retryable = ExecutionCancelResult.retryable("boom")
    assert retryable.status == "retryable"
    assert retryable.detail == "boom"
    assert retryable.permits_durable_cancel is False

    unsupported = ExecutionCancelResult.unsupported()
    assert unsupported.status == "unsupported"
    assert unsupported.permits_durable_cancel is True

    # Check that dataclasses.replace works
    replaced = replace(retryable, detail="new")
    assert replaced.detail == "new"

    # Check that setattr raises (frozen)
    with pytest.raises(AttributeError):
        accepted.status = "retryable"  # type: ignore[misc]


@pytest.mark.anyio
async def test_base_execution_backend_cancel_execution_is_unsupported() -> "None":
    from typing import cast

    from litestar_queues.config import WorkerConfig

    service = QueueService(
        QueueConfig(worker=WorkerConfig(placement="external"), queue_backend="memory"),
        queue_backend=InMemoryQueueBackend(),
    )
    await service.get_queue_backend().enqueue("tasks.unit")

    # We need a record, we can just pass a dummy or a real one.
    # The method ignores both arguments anyway.
    record = cast("QueuedTaskRecord", None)
    result = await BaseExecutionBackend().cancel_execution(service, record)
    assert result.status == "unsupported"


def test_the_dead_boolean_cancel_operation_is_gone() -> "None":
    assert not hasattr(BaseExecutionBackend, "cancel")
    assert not hasattr(CloudRunExecutionBackend, "cancel")

    import subprocess

    # Run grep to ensure no file defines async def cancel( in src/litestar_queues
    cmd = ["grep", "-rn", "async def cancel(", "src/litestar_queues"]
    res = subprocess.run(cmd, capture_output=True, text=True, check=False)
    assert res.stdout == ""


@pytest.mark.anyio
async def test_cancel_task_degradation_without_execution_backend() -> "None":
    from litestar_queues.task import task

    @task("tasks.ext")
    async def _ext() -> "None":
        return None

    from litestar_queues.config import WorkerConfig

    queue_backend = InMemoryQueueBackend()
    service = QueueService(
        QueueConfig(worker=WorkerConfig(placement="external"), queue_backend="memory"), queue_backend=queue_backend
    )
    async with service:
        record = await queue_backend.enqueue("tasks.ext")
        assert record.status == "pending"
        assert record.execution_backend == "local"

        # Test degradation fallback
        assert await service.cancel_task(record.id) is True

        stored = await queue_backend.get_task(record.id)
        assert stored and stored.status == "cancelled"

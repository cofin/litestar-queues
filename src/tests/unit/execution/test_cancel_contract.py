from dataclasses import replace

import pytest

from litestar_queues.backends.memory.backend import InMemoryQueueBackend
from litestar_queues.config import QueueConfig
from litestar_queues.execution.base import BaseExecutionBackend, ExecutionCancelResult
from litestar_queues.execution.cloudrun.backend import CloudRunExecutionBackend
from litestar_queues.service import QueueService


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
        accepted.status = "retryable"


@pytest.mark.anyio
async def test_base_execution_backend_cancel_execution_is_unsupported() -> "None":
    from litestar_queues.config import WorkerConfig
    service = QueueService(QueueConfig(worker=WorkerConfig(placement="external"), queue_backend="memory"), queue_backend=InMemoryQueueBackend())
    await service.get_queue_backend().enqueue("tasks.unit")

    # We need a record, we can just pass a dummy or a real one.
    # The method ignores both arguments anyway.
    record = None
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

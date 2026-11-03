"""Supplying attempt-scoped resources and stateless dependencies to tasks."""

from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager

from litestar_queues import QueueConfig, QueueService, Task, TaskExecutionContext, WorkerConfig, task
from litestar_queues.models import QueuedTaskRecord


class BillingClient:
    """Example scoped client managed per task execution attempt."""

    __slots__ = ("closed", "prefix")

    def __init__(self, prefix: str) -> None:
        self.prefix = prefix
        self.closed = False

    async def charge(self, invoice_id: str) -> str:
        """Issue a charge reference."""
        return f"{self.prefix}:{invoice_id}"


@task("billing.capture_payment", queue="payments")
async def capture_payment(invoice_id: str, billing_client: BillingClient) -> str:
    """Execute a task receiving only the parameters declared in its signature."""
    return await billing_client.charge(invoice_id)


@asynccontextmanager
async def provide_task_dependencies(
    task: Task[..., object], record: QueuedTaskRecord, context: TaskExecutionContext
) -> AsyncIterator[Mapping[str, object]]:
    """Yield attempt-scoped dependencies and guarantee teardown on exit."""
    _ = (task, context)
    client = BillingClient(prefix=record.queue)
    try:
        yield {"billing_client": client, "unused_dependency": "filtered-out"}
    finally:
        client.closed = True


async def main() -> None:
    """Verify attempt-scoped dependency injection and automatic signature filtering."""
    config = QueueConfig(
        queue_backend="memory",
        execution_backend="immediate",
        worker=WorkerConfig(placement="external"),
        task_dependency_provider=provide_task_dependencies,
    )
    async with QueueService(config=config) as service:
        handle = await service.enqueue(capture_payment, "inv-500")
        await handle.refresh()
        assert handle.status == "completed"
        assert handle.result == "payments:inv-500"

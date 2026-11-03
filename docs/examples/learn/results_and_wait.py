"""Waiting for task completion and refreshing TaskResult handles."""

from litestar_queues import QueueConfig, QueueService, TaskExitCode, WorkerConfig, consume_one, task


@task("analytics.compute_summary", queue="analytics")
async def compute_summary(values: list[int]) -> dict[str, int]:
    """Compute summary statistics for a batch of integers."""
    return {"count": len(values), "total": sum(values)}


async def main() -> None:
    """Enqueue a task, claim it with consume_one, and await the TaskResult."""
    config = QueueConfig(
        queue_backend="memory",
        execution_backend="local",
        worker=WorkerConfig(placement="external", queues=("analytics",)),
    )
    async with QueueService(config=config) as service:
        handle = await service.enqueue(compute_summary, [10, 20, 30])
        assert handle.status == "pending"

        exit_code = await consume_one(service, handle.id)
        assert exit_code == TaskExitCode.SUCCESS

        completed = await handle.wait(timeout=2.0)
        assert completed.status == "completed"
        assert completed.result == {"count": 3, "total": 60}
        assert completed.error is None

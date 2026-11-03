"""Testing tasks with immediate execution and deterministic consume_one claims."""

from litestar_queues import QueueConfig, QueueService, TaskExitCode, WorkerConfig, consume_one, task

EXPECTED_DIRECT_TAX = 825
EXPECTED_IMMEDIATE_TAX = 1650
EXPECTED_QUEUED_TAX = 4125


@task("orders.calculate_tax", queue="orders")
async def calculate_tax(subtotal_cents: int, rate_bps: int) -> int:
    """Compute tax in cents from a subtotal and basis-point rate."""
    return (subtotal_cents * rate_bps) // 10_000


async def main() -> None:
    """Demonstrate direct callable invocation, immediate execution, and consume_one."""
    direct_value = await calculate_tax(10_000, 825)
    assert direct_value == EXPECTED_DIRECT_TAX

    immediate_config = QueueConfig(
        queue_backend="memory", execution_backend="immediate", worker=WorkerConfig(placement="external")
    )
    async with QueueService(config=immediate_config) as service:
        handle = await service.enqueue(calculate_tax, 20_000, 825)
        await handle.refresh()
        assert handle.status == "completed"
        assert handle.result == EXPECTED_IMMEDIATE_TAX

    local_config = QueueConfig(
        queue_backend="memory", execution_backend="local", worker=WorkerConfig(placement="external")
    )
    async with QueueService(config=local_config) as service:
        queued = await service.enqueue(calculate_tax, 50_000, 825)
        assert queued.status == "pending"
        exit_code = await consume_one(service, queued.id)
        assert exit_code == TaskExitCode.SUCCESS
        await queued.refresh()
        assert queued.result == EXPECTED_QUEUED_TAX

"""Configuring task priority, delays, expiration, timeouts, and uniqueness reservations."""

from datetime import timedelta

from litestar_queues import QueueConfig, QueueService, WorkerConfig, task


@task(
    "billing.charge_invoice",
    queue="billing",
    priority=10,
    timeout=30.0,
    expires_in=timedelta(minutes=15),
    unique_by="arguments",
    unique_until="forever",
)
async def charge_invoice(invoice_id: str, amount_cents: int) -> dict[str, object]:
    """Charge an invoice at most once per unique argument signature."""
    return {"invoice_id": invoice_id, "amount_cents": amount_cents, "charged": True}


async def main() -> None:
    """Verify argument-based forever uniqueness and explicit reservation reset."""
    config = QueueConfig(
        queue_backend="memory", execution_backend="immediate", worker=WorkerConfig(placement="external")
    )
    async with QueueService(config=config) as service:
        first = await service.enqueue(charge_invoice, "inv-1001", 2500)
        duplicate = await service.enqueue(charge_invoice, "inv-1001", 2500)
        assert first.id == duplicate.id

        await first.refresh()
        assert first.record is not None
        assert first.record.key is not None

        reservation = await service.get_task_identity(first.record.key)
        assert reservation is not None
        assert reservation.task_id == first.id

        was_reset = await service.reset_task_identity(first.record.key)
        assert was_reset is True

        requeued = await service.enqueue(charge_invoice, "inv-1001", 2500)
        assert requeued.id != first.id

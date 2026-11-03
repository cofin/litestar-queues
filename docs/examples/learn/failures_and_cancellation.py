"""Configuring retry backoff, non-retryable failures, and task cancellation."""

from litestar_queues import (
    QueueConfig,
    QueueService,
    RetryBackoff,
    TaskExitCode,
    WorkerConfig,
    consume_one,
    get_current_task_context,
    job_cancelled,
    non_retryable,
    task,
)


@task(
    "imports.validate_payload",
    queue="imports",
    retries=3,
    retry_backoff=RetryBackoff(initial_delay=1.0, max_delay=30.0, multiplier=2.0),
)
async def validate_payload(mode: str) -> str:
    """Validate an import payload and demonstrate terminal vs retryable exits."""
    context = get_current_task_context()
    if context is not None:
        context.raise_if_cancelled()
    if mode == "invalid-schema":
        non_retryable("Payload schema version is unsupported")
    if mode == "abort":
        job_cancelled("Import aborted by operator policy")
    return "accepted"


async def main() -> None:
    """Verify non-retryable failure and queued cancellation transitions."""
    config = QueueConfig(
        queue_backend="memory",
        execution_backend="local",
        worker=WorkerConfig(placement="external", queues=("imports",)),
    )
    async with QueueService(config=config) as service:
        bad_handle = await service.enqueue(validate_payload, "invalid-schema")
        exit_code = await consume_one(service, bad_handle.id)
        assert exit_code == TaskExitCode.FAILURE
        await bad_handle.refresh()
        assert bad_handle.status == "failed"
        assert bad_handle.record is not None
        assert bad_handle.record.retry_count == 0

        queued_handle = await service.enqueue(validate_payload, "ok")
        was_cancelled = await service.cancel_task(queued_handle.id)
        assert was_cancelled is True
        await queued_handle.refresh()
        assert queued_handle.status == "cancelled"

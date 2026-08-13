"""Shared worker-ownership and shutdown-interrupt assertions for every queue backend."""

import asyncio
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from litestar_queues import (
    EventDeliveryConfig,
    InMemoryQueueEventSink,
    QueueConfig,
    QueueService,
    Worker,
    WorkerConfig,
    task,
)
from litestar_queues.events import QueueEventsConfig
from litestar_queues.task import clear_task_registry

if TYPE_CHECKING:
    from litestar_queues.backends import BaseQueueBackend


async def assert_assign_worker_persists_ownership(queue_backend: "BaseQueueBackend") -> "None":
    """Worker ownership survives a round trip and is fenced on the retry generation."""
    record = await queue_backend.enqueue("tasks.owned")
    claimed = await queue_backend.claim_task(record.id)
    assert claimed is not None
    generation = claimed.retry_count

    assigned = await queue_backend.assign_worker(record.id, worker_id="worker-a", expected_retry_count=generation)
    assert assigned is not None
    assert assigned.worker_id == "worker-a"

    stored = await queue_backend.get_task(record.id)
    assert stored is not None
    assert stored.worker_id == "worker-a"

    lost_fence = await queue_backend.assign_worker(
        record.id, worker_id="worker-b", expected_retry_count=generation + 1
    )
    assert lost_fence is None
    still_owned = await queue_backend.get_task(record.id)
    assert still_owned is not None
    assert still_owned.worker_id == "worker-a"


async def assert_interrupts_owned_running_record(queue_backend: "BaseQueueBackend") -> "None":
    """``interrupt_task`` returns an owned running attempt to pending, fenced on owner and generation."""
    record = await queue_backend.enqueue("tasks.interrupt", priority=7)
    claimed = await queue_backend.claim_task(record.id)
    assert claimed is not None
    # The memory backend hands back the live record, so snapshot the claimed
    # generation before any mutation instead of re-reading it later.
    generation = claimed.retry_count
    assigned = await queue_backend.assign_worker(record.id, worker_id="worker-a", expected_retry_count=generation)
    assert assigned is not None

    requeue_time = datetime.now(timezone.utc)
    wrong_owner = await queue_backend.interrupt_task(
        record.id, expected_retry_count=generation, worker_id="worker-b", queued_at=requeue_time
    )
    assert wrong_owner is None
    wrong_generation = await queue_backend.interrupt_task(
        record.id, expected_retry_count=generation + 1, worker_id="worker-a", queued_at=requeue_time
    )
    assert wrong_generation is None

    updated = await queue_backend.interrupt_task(
        record.id, expected_retry_count=generation, worker_id="worker-a", queued_at=requeue_time
    )
    assert updated is not None
    assert updated.status == "pending"
    assert updated.retry_count == generation + 1
    assert updated.metadata.get("interruptions") == 1

    # The generation bump locks the previous owner out of every settle fence.
    assert (
        await queue_backend.complete_task(record.id, result="stale-owner", expected_retry_count=generation)
        is None
    )

    stored = await queue_backend.get_task(record.id)
    assert stored is not None
    assert stored.status == "pending"
    assert stored.priority == 7
    assert stored.scheduled_at is None
    assert stored.started_at is None
    assert stored.heartbeat_at is None
    assert stored.completed_at is None
    assert stored.execution_ref is None
    assert stored.worker_id is None

    reclaimed = await queue_backend.claim_task(record.id)
    assert reclaimed is not None


async def assert_worker_shutdown_requeues_running_task(queue_backend: "BaseQueueBackend") -> "None":
    """``requeue_on_shutdown`` returns an in-flight attempt to pending and reports the interruption."""
    clear_task_registry()
    started = asyncio.Event()
    sink = InMemoryQueueEventSink()

    @task("shutdown.requeue.blocking")
    async def blocking() -> "None":
        started.set()
        await asyncio.Event().wait()

    config = QueueConfig(
        worker=WorkerConfig(placement="external"),
        queue_backend="memory",
        execution_backend="local",
        events=QueueEventsConfig(delivery=EventDeliveryConfig(sinks=(sink,))),
    )
    service = QueueService(config, queue_backend=queue_backend)
    async with service:
        result = await service.enqueue(blocking)
        enqueued = await queue_backend.get_task(result.id)
        assert enqueued is not None
        worker = Worker(
            service,
            WorkerConfig(
                placement="external",
                requeue_on_shutdown=True,
                max_concurrency=1,
                graceful_shutdown_timeout=0.05,
                final_cancel_timeout=5.0,
                poll_interval=0.05,
                poll_backoff_max=None,
            ),
        )
        worker_task = asyncio.create_task(worker.start())
        await asyncio.wait_for(started.wait(), timeout=10)

        escalated = await asyncio.wait_for(worker.stop(), timeout=15)
        await asyncio.wait_for(worker_task, timeout=15)

        stored = await queue_backend.get_task(result.id)

    assert escalated is True
    assert stored is not None
    assert stored.status == "pending"
    assert stored.started_at is None
    assert stored.heartbeat_at is None
    assert stored.worker_id is None
    assert stored.queued_at >= enqueued.queued_at
    assert any(event.type == "task.interrupted" for event in sink.events)


async def assert_interruption_does_not_consume_retry_budget(queue_backend: "BaseQueueBackend") -> "None":
    """An interruption bumps the fence generation without spending a retry attempt."""
    record = await queue_backend.enqueue("tasks.interrupt.budget", max_retries=1)
    claimed = await queue_backend.claim_task(record.id)
    assert claimed is not None
    generation = claimed.retry_count
    assert await queue_backend.assign_worker(record.id, worker_id="worker-a", expected_retry_count=generation)
    interrupted = await queue_backend.interrupt_task(
        record.id, expected_retry_count=generation, worker_id="worker-a", queued_at=datetime.now(timezone.utc)
    )
    assert interrupted is not None
    assert interrupted.retry_count == generation + 1

    reclaimed = await queue_backend.claim_task(record.id)
    assert reclaimed is not None

    result = await queue_backend.requeue_stale_running(stale_after=timedelta(seconds=-2))
    stored = await queue_backend.get_task(record.id)

    assert result.requeued == 1
    assert result.failed == 0
    assert stored is not None
    assert stored.status in {"pending", "scheduled"}


async def assert_interruption_does_not_consume_failure_budget(queue_backend: "BaseQueueBackend") -> "None":
    """A failure after an interruption still gets the record's full retry budget."""
    record = await queue_backend.enqueue("tasks.interrupt.fail_budget", max_retries=1)
    claimed = await queue_backend.claim_task(record.id)
    assert claimed is not None
    generation = claimed.retry_count
    assert await queue_backend.assign_worker(record.id, worker_id="worker-a", expected_retry_count=generation)
    interrupted = await queue_backend.interrupt_task(
        record.id, expected_retry_count=generation, worker_id="worker-a", queued_at=datetime.now(timezone.utc)
    )
    assert interrupted is not None

    reclaimed = await queue_backend.claim_task(record.id)
    assert reclaimed is not None
    failed = await queue_backend.fail_task(record.id, "boom", retry=True)

    assert failed is not None
    assert failed.status in {"pending", "scheduled"}

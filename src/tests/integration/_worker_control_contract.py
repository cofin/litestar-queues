"""Backend-agnostic proof that a control hint cancels a saturated worker.

The scenario pins both durable slow paths (adaptive-poll ceiling and the
cancellation poll cadence) far beyond the assertion window, so a prompt cancel
can only come from the backend's push worker-control transport. The companion
helper pins them short and disables the transport, proving the durable poll
remains the correctness fallback.
"""

import asyncio
from contextlib import suppress
from typing import TYPE_CHECKING

from litestar_queues import QueueConfig, QueueService, Worker, WorkerConfig, task

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from litestar_queues.backends import BaseQueueBackend

__all__ = ("assert_control_hint_cancels_saturated_worker", "assert_durable_poll_cancels_without_control_hint")

_PINNED_FAR = 30.0
"""Poll and cancellation-poll interval used to rule out durable discovery."""


async def _run_saturated_cancel(
    *,
    worker_backend: "BaseQueueBackend",
    control_backend: "BaseQueueBackend",
    backend_name: "str",
    cancellation_poll_interval: "float",
    poll_interval: "float",
    wait_for_listener: "Callable[[], Awaitable[None]] | None",
    timeout: "float",
) -> "None":
    started = asyncio.Event()
    cancelled = asyncio.Event()

    @task("tasks.worker_control_hint")
    async def hung() -> "None":
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    config = QueueConfig(
        worker=WorkerConfig(placement="external"),
        queue_backend=backend_name,
        execution_backend="local",
        initialize_schedules=False,
    )
    service = await QueueService(config, queue_backend=worker_backend).open()
    controller = await QueueService(config, queue_backend=control_backend).open()
    worker = Worker(
        service,
        WorkerConfig(
            max_concurrency=1,
            poll_interval=poll_interval,
            poll_backoff_max=None,
            cancellation_poll_interval=cancellation_poll_interval,
            reconcile_interval=3600,
            heartbeat_interval=60,
        ),
    )

    record = await service.enqueue(hung)
    worker_task = asyncio.create_task(worker.start())
    try:
        await asyncio.wait_for(started.wait(), timeout=timeout)
        if wait_for_listener is not None:
            await wait_for_listener()

        assert await controller.cancel_task(record.id, include_running=True) is True
        await asyncio.wait_for(cancelled.wait(), timeout=timeout)
    finally:
        await worker.stop()
        try:
            await asyncio.wait_for(asyncio.shield(worker_task), timeout=timeout)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            worker_task.cancel()
            with suppress(asyncio.CancelledError, asyncio.TimeoutError):
                await worker_task

    stored = await worker_backend.get_task(record.id)
    assert stored is not None
    assert stored.status == "cancelled"


async def assert_control_hint_cancels_saturated_worker(
    *,
    worker_backend: "BaseQueueBackend",
    control_backend: "BaseQueueBackend",
    backend_name: "str",
    wait_for_listener: "Callable[[], Awaitable[None]] | None" = None,
    timeout: "float" = 10.0,
) -> "None":
    """Assert a cross-instance cancel reaches a saturated worker within ``timeout``."""
    await _run_saturated_cancel(
        worker_backend=worker_backend,
        control_backend=control_backend,
        backend_name=backend_name,
        cancellation_poll_interval=_PINNED_FAR,
        poll_interval=_PINNED_FAR,
        wait_for_listener=wait_for_listener,
        timeout=timeout,
    )


async def assert_durable_poll_cancels_without_control_hint(
    *,
    worker_backend: "BaseQueueBackend",
    control_backend: "BaseQueueBackend",
    backend_name: "str",
    timeout: "float" = 10.0,
) -> "None":
    """Assert the durable status poll still cancels when every hint is dropped."""
    backend_type = type(worker_backend)
    original = backend_type.wait_for_worker_control

    async def never_delivers(self: "BaseQueueBackend", *, worker_id: "str", timeout: "float | None" = None) -> "bool":
        del self, worker_id
        if timeout is not None:
            await asyncio.sleep(timeout)
        return False

    # Slot classes reject instance-level method assignment, so patch the class.
    backend_type.wait_for_worker_control = never_delivers  # type: ignore[method-assign]
    try:
        await _run_saturated_cancel(
            worker_backend=worker_backend,
            control_backend=control_backend,
            backend_name=backend_name,
            cancellation_poll_interval=0.1,
            poll_interval=0.1,
            wait_for_listener=None,
            timeout=timeout,
        )
    finally:
        backend_type.wait_for_worker_control = original  # type: ignore[method-assign]

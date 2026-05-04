import asyncio
import contextlib
from datetime import timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from uuid import UUID

    from litestar_queues.models import QueuedTaskRecord
    from litestar_queues.service import QueueService

__all__ = ("Worker",)


class Worker:
    """Local in-process queue worker."""

    __slots__ = (
        "_batch_size",
        "_final_cancel_timeout",
        "_graceful_shutdown_timeout",
        "_heartbeat_interval",
        "_is_running",
        "_max_concurrency",
        "_poll_interval",
        "_service",
        "_stale_after",
        "_stop_event",
    )

    def __init__(
        self,
        service: "QueueService",
        *,
        batch_size: int = 10,
        poll_interval: float = 0.1,
        max_concurrency: int = 1,
        heartbeat_interval: float = 30,
        stale_after: timedelta | None = None,
        graceful_shutdown_timeout: float = 30,
        final_cancel_timeout: float = 5,
    ) -> None:
        """Initialize the worker."""
        self._service = service
        self._batch_size = batch_size
        self._poll_interval = poll_interval
        self._max_concurrency = max(1, max_concurrency)
        self._heartbeat_interval = heartbeat_interval
        self._stale_after = stale_after
        self._graceful_shutdown_timeout = graceful_shutdown_timeout
        self._final_cancel_timeout = final_cancel_timeout
        self._stop_event = asyncio.Event()
        self._is_running = False

    @property
    def is_running(self) -> bool:
        """Return whether the worker loop is active."""
        return self._is_running

    async def start(self) -> None:
        """Run the worker loop until stopped or cancelled."""
        self._is_running = True
        self._stop_event.clear()
        try:
            if self._stale_after is not None:
                await self._service.get_queue_backend().requeue_stale_running(stale_after=self._stale_after)
            while not self._stop_event.is_set():
                processed = await self.run_once()
                if processed == 0:
                    await self._wait_for_work()
        finally:
            self._is_running = False

    async def stop(self) -> None:
        """Stop the worker loop."""
        self._stop_event.set()

    async def run_once(self) -> int:
        """Process one batch of due tasks.

        Returns:
            Number of claimed task records.
        """
        queue_backend = self._service.get_queue_backend()
        records = await queue_backend.list_pending(
            limit=self._batch_size,
            execution_backend=self._service.config.execution_backend,
        )
        claimed_records: list["QueuedTaskRecord"] = []
        for record in records:
            claimed = await queue_backend.claim_task(record.id)
            if claimed is None:
                continue
            claimed_records.append(claimed)
        if not claimed_records:
            return 0

        semaphore = asyncio.Semaphore(self._max_concurrency)

        async def run_claimed(claimed_record: "QueuedTaskRecord") -> None:
            async with semaphore:
                await self._execute_claimed(claimed_record)

        await asyncio.gather(*(run_claimed(record) for record in claimed_records))
        return len(claimed_records)

    async def _execute_claimed(self, record: "QueuedTaskRecord") -> None:
        heartbeat_task = asyncio.create_task(self._heartbeat(record.id))
        try:
            await self._service.get_execution_backend().execute(self._service, record)
        finally:
            heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat_task
            await self._service.get_queue_backend().null_heartbeats([record.id])

    async def _heartbeat(self, task_id: "UUID") -> None:
        while True:
            await asyncio.sleep(self._heartbeat_interval)
            await self._service.get_queue_backend().touch_heartbeat(task_id)

    async def _wait_for_work(self) -> None:
        queue_backend = self._service.get_queue_backend()
        notification_task = asyncio.create_task(queue_backend.wait_for_notifications(timeout=self._poll_interval))
        stop_task = asyncio.create_task(self._stop_event.wait())
        done, pending = await asyncio.wait(
            {notification_task, stop_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        for task in done:
            with contextlib.suppress(TimeoutError):
                task.result()

import asyncio
import contextlib
import os
from datetime import timedelta
from typing import TYPE_CHECKING

from litestar_queues.config import execution_backend_name
from litestar_queues.execution import get_execution_backend

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
        "_last_reconcile_at",
        "_last_stale_check_at",
        "_max_concurrency",
        "_poll_interval",
        "_queues",
        "_reconcile_interval",
        "_running_tasks",
        "_service",
        "_stale_after",
        "_stale_check_interval",
        "_stop_event",
        "_worker_id",
    )

    def __init__(
        self,
        service: "QueueService",
        *,
        batch_size: int = 10,
        poll_interval: float = 0.1,
        max_concurrency: int = 1,
        heartbeat_interval: float = 30,
        reconcile_interval: float = 30,
        stale_after: timedelta | None = None,
        stale_check_interval: float = 60.0,
        graceful_shutdown_timeout: float = 30,
        final_cancel_timeout: float = 5,
        worker_id: str | None = None,
        queues: tuple[str, ...] = (),
    ) -> None:
        """Initialize the worker."""
        self._service = service
        self._batch_size = batch_size
        self._poll_interval = poll_interval
        self._max_concurrency = max(1, max_concurrency)
        self._heartbeat_interval = heartbeat_interval
        self._reconcile_interval = reconcile_interval
        self._stale_after = stale_after
        self._stale_check_interval = stale_check_interval
        self._graceful_shutdown_timeout = graceful_shutdown_timeout
        self._final_cancel_timeout = final_cancel_timeout
        self._worker_id = worker_id if worker_id is not None else f"worker-{os.getpid()}"
        self._queues = queues
        self._running_tasks: set[asyncio.Task[None]] = set()
        self._stop_event = asyncio.Event()
        self._is_running = False
        self._last_reconcile_at = 0.0
        self._last_stale_check_at = 0.0

    @property
    def is_running(self) -> bool:
        """Return whether the worker loop is active."""
        return self._is_running

    @property
    def worker_id(self) -> str:
        """Return the worker identity used for events and logs."""
        return self._worker_id

    async def start(self) -> None:
        """Run the worker loop until stopped or cancelled."""
        self._is_running = True
        self._stop_event.clear()
        try:
            while not self._stop_event.is_set():
                await self._maybe_requeue_stale()
                await self._maybe_reconcile_external()
                processed = await self.run_once()
                if processed == 0:
                    await self._wait_for_work()
        finally:
            self._is_running = False

    async def stop(self, *, force: bool = False) -> None:
        """Stop the worker loop and drain or cancel in-flight work."""
        self._stop_event.set()
        if force:
            await self._cancel_running()
            return
        await self._drain_running()

    async def run_once(self) -> int:
        """Process one batch of due tasks.

        Returns:
            Number of claimed task records.
        """
        queue_backend = self._service.get_queue_backend()
        execution_backend = self._service.get_execution_backend()
        available = min(self._batch_size, max(0, self._max_concurrency - len(self._running_tasks)))
        if available <= 0:
            return 0
        records = await self._list_pending(limit=available)
        if execution_backend.is_external:
            return await self._dispatch_external(records)

        claimed_records: list["QueuedTaskRecord"] = []
        for record in records:
            claimed = await queue_backend.claim_task(record.id)
            if claimed is None:
                continue
            claimed_records.append(claimed)
        if not claimed_records:
            return 0

        tasks = [self._track_execution(record) for record in claimed_records]
        await asyncio.gather(*tasks, return_exceptions=True)
        return len(claimed_records)

    async def _list_pending(self, *, limit: int) -> "list[QueuedTaskRecord]":
        queue_backend = self._service.get_queue_backend()
        execution_backend_name_ = execution_backend_name(self._service.config.execution_backend)
        if not self._queues:
            return await queue_backend.list_pending(limit=limit, execution_backend=execution_backend_name_)

        records: list["QueuedTaskRecord"] = []
        seen: set[object] = set()
        for queue in self._queues:
            if len(records) >= limit:
                break
            queue_records = await queue_backend.list_pending(
                limit=limit - len(records), queue=queue, execution_backend=execution_backend_name_
            )
            for record in queue_records:
                if record.id in seen:
                    continue
                seen.add(record.id)
                records.append(record)
                if len(records) >= limit:
                    break
        return records

    def _track_execution(self, record: "QueuedTaskRecord") -> asyncio.Task[None]:
        task = asyncio.create_task(self._execute_claimed(record))
        self._running_tasks.add(task)
        task.add_done_callback(self._running_tasks.discard)
        return task

    async def reconcile_external(self, *, limit: int | None = None) -> int:
        """Reconcile externally dispatched records.

        Returns:
            Number of records that reached a terminal queue status.
        """
        queue_backend = self._service.get_queue_backend()
        records = await queue_backend.list_running_external(limit=limit)
        reconciled = 0
        current_backend = self._service.get_execution_backend()
        for record in records:
            if record.execution_ref is None:
                continue
            execution_backend = (
                current_backend
                if record.execution_backend == execution_backend_name(self._service.config.execution_backend)
                else get_execution_backend(record.execution_backend, config=self._service.config)
            )
            updated = await execution_backend.reconcile(self._service, record)
            if updated is not None and updated.is_terminal:
                reconciled += 1
        return reconciled

    async def _execute_claimed(self, record: "QueuedTaskRecord") -> None:
        heartbeat_task = asyncio.create_task(self._heartbeat(record.id, expected_retry_count=record.retry_count))
        try:
            await self._service.get_execution_backend().execute(self._service, record, worker_id=self._worker_id)
        finally:
            heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat_task
            await self._service.get_queue_backend().null_heartbeats(
                [record.id], expected_retry_count=record.retry_count
            )

    async def _dispatch_external(self, records: "list[QueuedTaskRecord]") -> int:
        execution_backend = self._service.get_execution_backend()
        dispatched = 0
        for record in records:
            if record.execution_ref is not None:
                continue
            await execution_backend.dispatch(self._service, record)
            dispatched += 1
        return dispatched

    async def _maybe_requeue_stale(self) -> None:
        if self._stale_after is None:
            return
        now = asyncio.get_running_loop().time()
        if now - self._last_stale_check_at < self._stale_check_interval:
            return
        self._last_stale_check_at = now
        await self._service.recover_stale_tasks(stale_after=self._stale_after, worker_id=self._worker_id)

    async def _maybe_reconcile_external(self) -> None:
        if self._reconcile_interval <= 0:
            await self.reconcile_external()
            return
        now = asyncio.get_running_loop().time()
        if now - self._last_reconcile_at < self._reconcile_interval:
            return
        self._last_reconcile_at = now
        await self.reconcile_external()

    async def _heartbeat(self, task_id: "UUID", expected_retry_count: int | None = None) -> None:
        while True:
            await asyncio.sleep(self._heartbeat_interval)
            await self._service.get_queue_backend().touch_heartbeat(task_id, expected_retry_count=expected_retry_count)

    async def _drain_running(self) -> None:
        if not self._running_tasks:
            return
        try:
            await asyncio.wait_for(
                asyncio.gather(*tuple(self._running_tasks), return_exceptions=True),
                timeout=self._graceful_shutdown_timeout,
            )
        except TimeoutError:
            await self._cancel_running()

    async def _cancel_running(self) -> None:
        tasks = tuple(self._running_tasks)
        if not tasks:
            return
        for task in tasks:
            task.cancel()
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=self._final_cancel_timeout)

    async def _wait_for_work(self) -> None:
        queue_backend = self._service.get_queue_backend()
        notification_task = asyncio.create_task(queue_backend.wait_for_notifications(timeout=self._poll_interval))
        stop_task = asyncio.create_task(self._stop_event.wait())
        done, pending = await asyncio.wait({notification_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        for task in done:
            with contextlib.suppress(TimeoutError):
                task.result()

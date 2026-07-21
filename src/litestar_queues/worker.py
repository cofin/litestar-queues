import asyncio
import contextlib
import logging
import os
import time
from datetime import timedelta
from typing import TYPE_CHECKING

from litestar_queues._heartbeat import WorkerHeartbeatManager
from litestar_queues.config import execution_backend_name
from litestar_queues.events.context import _bind_beat_sink, _reset_beat_sink
from litestar_queues.execution import get_execution_backend

if TYPE_CHECKING:
    from litestar_queues.models import QueuedTaskRecord
    from litestar_queues.service import QueueService

__all__ = ("Worker",)

logger = logging.getLogger(__name__)


class Worker:
    """Local in-process queue worker."""

    __slots__ = (
        "_batch_size",
        "_completion_event",
        "_final_cancel_timeout",
        "_graceful_shutdown_timeout",
        "_heartbeat_manager",
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
        batch_size: "int" = 10,
        poll_interval: "float" = 0.1,
        max_concurrency: "int" = 1,
        heartbeat_interval: "float" = 30,
        heartbeat_miss_threshold: "int" = 2,
        reconcile_interval: "float" = 30,
        stale_after: "timedelta | None" = None,
        stale_check_interval: "float" = 60.0,
        graceful_shutdown_timeout: "float" = 30,
        final_cancel_timeout: "float" = 5,
        worker_id: "str | None" = None,
        queues: "tuple[str, ...]" = (),
    ) -> "None":
        """Initialize the worker."""
        self._service = service
        self._batch_size = batch_size
        self._poll_interval = poll_interval
        self._max_concurrency = max(1, max_concurrency)
        self._reconcile_interval = reconcile_interval
        self._stale_after = stale_after
        self._stale_check_interval = stale_check_interval
        self._graceful_shutdown_timeout = graceful_shutdown_timeout
        self._final_cancel_timeout = final_cancel_timeout
        self._worker_id = worker_id if worker_id is not None else f"worker-{os.getpid()}"
        self._heartbeat_manager = WorkerHeartbeatManager(
            service, interval=heartbeat_interval, miss_threshold=heartbeat_miss_threshold, worker_id=self._worker_id
        )
        self._queues = queues
        self._running_tasks: "set[asyncio.Task[None]]" = set()
        self._stop_event = asyncio.Event()
        self._completion_event = asyncio.Event()
        self._is_running = False
        self._last_reconcile_at = -float("inf")
        self._last_stale_check_at = -float("inf")

    @property
    def is_running(self) -> "bool":
        """Whether the worker loop is active."""
        return self._is_running

    @property
    def worker_id(self) -> "str":
        """Worker identity used for events and logs."""
        return self._worker_id

    async def start(self) -> "None":
        """Run the worker loop until stopped or cancelled."""
        self._is_running = True
        self._stop_event.clear()
        try:
            await self._heartbeat_manager.start()
            while not self._stop_event.is_set():
                try:
                    await self._maybe_requeue_stale()
                    await self._maybe_reconcile_external()
                    processed = await self.run_once()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self._record_counter(
                        "litestar_queues.worker.loop.error.count", {"worker.error.type": type(exc).__name__}
                    )
                    logger.exception("Queue worker loop iteration failed", extra={"worker_id": self._worker_id})
                    await self._backoff_after_loop_error()
                    continue
                if processed == 0:
                    await self._wait_for_work()
        finally:
            self._stop_event.set()
            try:
                await self._drain_running()
            finally:
                try:
                    await self._heartbeat_manager.aclose()
                finally:
                    self._is_running = False

    async def stop(self, *, force: "bool" = False) -> "bool":
        """Stop the worker loop and drain or cancel in-flight work.

        Returns:
            True when graceful drain escalated to cancellation.
        """
        self._stop_event.set()
        if force:
            await self._cancel_running()
            return False
        return await self._drain_running()

    async def run_once(self) -> "int":
        """Process one batch of due tasks.

        Returns:
            Number of claimed task records.
        """
        execution_backend = self._service.get_execution_backend()
        available = min(self._batch_size, max(0, self._max_concurrency - len(self._running_tasks)))
        if available <= 0:
            return 0
        if execution_backend.is_external:
            records = await self._list_pending(limit=available)
            return await self._dispatch_external(records)

        claimed_records = await self._claim_available(limit=available)
        if not claimed_records:
            return 0

        self._record_claimed(claimed_records)
        for record in claimed_records:
            self._track_execution(record)
        return len(claimed_records)

    async def _claim_available(self, *, limit: "int") -> "list[QueuedTaskRecord]":
        queue_backend = self._service.get_queue_backend()
        execution_backend_name_ = execution_backend_name(self._service.config.execution_backend)
        return await queue_backend.claim_many(
            limit=limit, queues=self._queues, execution_backend=execution_backend_name_
        )

    async def _list_pending(self, *, limit: "int") -> "list[QueuedTaskRecord]":
        queue_backend = self._service.get_queue_backend()
        execution_backend_name_ = execution_backend_name(self._service.config.execution_backend)
        if not self._queues:
            return await queue_backend.list_pending(limit=limit, execution_backend=execution_backend_name_)

        records: 'list["QueuedTaskRecord"]' = []
        seen: "set[object]" = set()
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

    def _track_execution(self, record: "QueuedTaskRecord") -> "asyncio.Task[None]":
        task = asyncio.create_task(self._execute_claimed(record))
        self._running_tasks.add(task)
        task.add_done_callback(self._on_execution_done)
        return task

    def _on_execution_done(self, task: "asyncio.Task[None]") -> "None":
        self._running_tasks.discard(task)
        self._completion_event.set()

    async def reconcile_external(self, *, limit: "int | None" = None) -> "int":
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
            try:
                execution_backend = (
                    current_backend
                    if record.execution_backend == execution_backend_name(self._service.config.execution_backend)
                    else get_execution_backend(record.execution_backend, config=self._service.config)
                )
            except ValueError:
                logger.warning(
                    "Skipping external queue record with unknown execution backend",
                    extra={"task_id": str(record.id), "execution_backend": record.execution_backend},
                )
                continue
            updated = await execution_backend.reconcile(self._service, record)
            if updated is not None and updated.is_terminal:
                reconciled += 1
                self._record_counter(
                    "litestar_queues.execution.reconcile.count",
                    {"queue.task.status": updated.status, "queue.execution.backend": record.execution_backend},
                )
        return reconciled

    async def _execute_claimed(self, record: "QueuedTaskRecord") -> "None":
        await self._heartbeat_manager.start()
        self._heartbeat_manager.register(record.id, expected_retry_count=record.retry_count)
        beat_token = _bind_beat_sink(self._heartbeat_manager)
        try:
            await self._service.get_execution_backend().execute(self._service, record, worker_id=self._worker_id)
        finally:
            _reset_beat_sink(beat_token)
            try:
                try:
                    self._heartbeat_manager.unregister(record.id)
                except Exception as exc:  # noqa: BLE001 - heartbeat cleanup must not skip backend clearing.
                    self._record_heartbeat_failure(exc, "Queue task heartbeat cleanup failed")
            finally:
                await self._close_heartbeat_manager_if_idle()

    async def _dispatch_external(self, records: "list[QueuedTaskRecord]") -> "int":
        execution_backend = self._service.get_execution_backend()
        dispatched = 0
        for record in records:
            if record.execution_ref is not None:
                continue
            try:
                await execution_backend.dispatch(self._service, record)
            except Exception:
                self._record_counter(
                    "litestar_queues.execution.dispatch.count",
                    {
                        "messaging.destination.name": record.queue,
                        "queue.execution.backend": record.execution_backend,
                        "queue.execution.status": "error",
                    },
                )
                raise
            self._record_counter(
                "litestar_queues.execution.dispatch.count",
                {
                    "messaging.destination.name": record.queue,
                    "queue.execution.backend": record.execution_backend,
                    "queue.execution.status": "dispatched",
                },
            )
            dispatched += 1
        return dispatched

    async def _maybe_requeue_stale(self) -> "None":
        if self._stale_after is None:
            return
        now = asyncio.get_running_loop().time()
        if now - self._last_stale_check_at < self._stale_check_interval:
            return
        self._last_stale_check_at = now
        if not await self._service.get_queue_backend().acquire_worker_lock(
            "stale_recovery", ttl=timedelta(seconds=max(self._stale_check_interval, 1.0))
        ):
            return
        result = await self._service.recover_stale_tasks(stale_after=self._stale_after, worker_id=self._worker_id)
        total = result.requeued + result.failed + result.skipped + result.handler_needed
        if total:
            self._record_counter(
                "litestar_queues.stale_recovery.count",
                {
                    "queue.stale.requeued": str(result.requeued),
                    "queue.stale.failed": str(result.failed),
                    "queue.stale.skipped": str(result.skipped),
                    "queue.stale.handler_needed": str(result.handler_needed),
                },
                value=total,
            )

    async def _maybe_reconcile_external(self) -> "None":
        if not await self._service.get_queue_backend().acquire_worker_lock(
            "external_reconcile", ttl=timedelta(seconds=max(self._reconcile_interval, 1.0))
        ):
            return
        if self._reconcile_interval <= 0:
            await self.reconcile_external()
            return
        now = asyncio.get_running_loop().time()
        if now - self._last_reconcile_at < self._reconcile_interval:
            return
        self._last_reconcile_at = now
        await self.reconcile_external()

    async def _drain_running(self) -> "bool":
        if not self._running_tasks:
            return False
        try:
            await asyncio.wait_for(
                asyncio.gather(*tuple(self._running_tasks), return_exceptions=True),
                timeout=self._graceful_shutdown_timeout,
            )
        except asyncio.TimeoutError:
            await self._cancel_running()
            return True
        return False

    async def _cancel_running(self) -> "None":
        tasks = tuple(self._running_tasks)
        if not tasks:
            return
        for task in tasks:
            task.cancel()
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=self._final_cancel_timeout)

    async def _wait_for_work(self) -> "None":
        if self._completion_event.is_set():
            self._completion_event.clear()
            return
        queue_backend = self._service.get_queue_backend()
        started_at = time.perf_counter()
        notification_task = asyncio.create_task(queue_backend.wait_for_notifications(timeout=self._poll_interval))
        stop_task = asyncio.create_task(self._stop_event.wait())
        completion_task = asyncio.create_task(self._completion_event.wait())
        done, pending = await asyncio.wait(
            {notification_task, stop_task, completion_task}, return_when=asyncio.FIRST_COMPLETED
        )
        self._completion_event.clear()
        for task in pending:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        for task in done:
            await self._consume_wait_task(task)
        self._service.observability_runtime.record_duration(
            "litestar_queues.worker.idle.duration",
            time.perf_counter() - started_at,
            attributes={**self._worker_metric_base_attributes(), "worker.wakeup": str(notification_task in done)},
        )

    async def _consume_wait_task(self, task: "asyncio.Task[bool]") -> "None":
        try:
            task.result()
        except asyncio.TimeoutError:
            return
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # A native read failure must not kill the run loop; durable polling
            # (run_once) still discovers work, and the next wait re-establishes
            # the listener from a clean state.
            self._record_counter("litestar_queues.worker.loop.error.count", {"worker.error.type": type(exc).__name__})
            logger.exception("Queue worker loop iteration failed", extra={"worker_id": self._worker_id})
            await self._backoff_after_loop_error()

    async def _backoff_after_loop_error(self) -> "None":
        timeout = min(max(self._poll_interval, 0.01), 1.0)
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self._stop_event.wait(), timeout=timeout)

    async def _close_heartbeat_manager_if_idle(self) -> "None":
        if self._is_running or self._heartbeat_manager.has_registrations:
            return
        try:
            await self._heartbeat_manager.aclose()
        except Exception as exc:  # noqa: BLE001 - heartbeat cleanup must not fail completed task execution.
            self._record_heartbeat_failure(exc, "Queue worker heartbeat manager close failed")

    def _record_heartbeat_failure(self, exc: "Exception", message: "str") -> "None":
        with contextlib.suppress(Exception):
            self._record_counter("litestar_queues.heartbeat.failure.count", {"worker.error.type": type(exc).__name__})
        logger.warning(message, exc_info=(type(exc), exc, exc.__traceback__), extra={"worker_id": self._worker_id})

    def _record_claimed(self, records: "list[QueuedTaskRecord]") -> "None":
        counts: "dict[str, int]" = {}
        for record in records:
            counts[record.queue] = counts.get(record.queue, 0) + 1
        for queue, count in counts.items():
            self._record_counter(
                "litestar_queues.worker.claim.count", {"messaging.destination.name": queue}, value=count
            )

    def _record_counter(self, name: "str", attributes: "dict[str, str]", *, value: "int" = 1) -> "None":
        self._service.observability_runtime.record_counter(
            name, value, attributes={**self._worker_metric_base_attributes(), **attributes}
        )

    def _worker_metric_base_attributes(self) -> "dict[str, str]":
        return {"queue.execution.backend": execution_backend_name(self._service.config.execution_backend)}

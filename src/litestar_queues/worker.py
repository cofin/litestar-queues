import asyncio
import contextlib
import logging
import os
import random
import time
from datetime import timedelta
from typing import TYPE_CHECKING

from litestar_queues._heartbeat import WorkerHeartbeatManager
from litestar_queues.config import WorkerConfig, execution_backend_name
from litestar_queues.events.context import _bind_beat_sink, _reset_beat_sink

if TYPE_CHECKING:
    from litestar_queues.models import QueuedTaskRecord
    from litestar_queues.service import QueueService

__all__ = ("Worker",)

logger = logging.getLogger(__name__)


def _clamp(value: "float", *, low: "float", high: "float") -> "float":
    """Clamp a value between inclusive bounds.

    Returns:
        ``value`` restricted to ``[low, high]``.
    """
    return max(low, min(high, value))


def _next_backoff_interval(current: "float", *, base: "float", maximum: "float", multiplier: "float") -> "float":
    """Compute the next deterministic adaptive-polling interval after an empty cycle.

    The stored exponential state itself is never jittered; only the sampled
    wait derived from it is (see :func:`_apply_jitter`).

    Returns:
        ``current * multiplier`` clamped to ``[base, maximum]``.
    """
    return _clamp(current * multiplier, low=base, high=maximum)


def _sample_symmetric_jitter() -> "float":
    """Return a uniform random value in ``[-1.0, 1.0]`` for jitter sampling.

    Returns:
        A pseudo-random value used only to perturb a sampled wait.
    """
    return random.uniform(-1.0, 1.0)  # noqa: S311 - jitter is not security sensitive.


def _apply_jitter(interval: "float", *, base: "float", maximum: "float", jitter: "float") -> "float":
    """Apply bounded symmetric jitter to a sampled wait, without mutating stored state.

    Returns:
        ``interval`` offset by up to ``interval * jitter``, clamped to ``[base, maximum]``.
    """
    if jitter <= 0.0:
        return interval
    offset = interval * jitter * _sample_symmetric_jitter()
    return _clamp(interval + offset, low=base, high=maximum)


class Worker:
    """Local in-process queue worker."""

    __slots__ = (
        "_batch_size",
        "_completion_event",
        "_current_poll_interval",
        "_final_cancel_timeout",
        "_graceful_shutdown_timeout",
        "_heartbeat_manager",
        "_is_running",
        "_last_reconcile_at",
        "_last_stale_check_at",
        "_max_concurrency",
        "_poll_backoff_max",
        "_poll_backoff_multiplier",
        "_poll_interval",
        "_poll_jitter",
        "_queues",
        "_reconcile_interval",
        "_running_tasks",
        "_service",
        "_stale_after",
        "_stale_check_interval",
        "_stop_event",
        "_worker_id",
    )

    def __init__(self, service: "QueueService", config: "WorkerConfig | None" = None) -> "None":
        """Initialize the worker.

        Args:
            service: Queue service used to reach the configured backends.
            config: Worker runtime configuration; ``None`` uses defaults.
        """
        worker_config = config or WorkerConfig()
        self._service = service
        self._batch_size = worker_config.batch_size
        self._poll_interval = worker_config.poll_interval
        self._poll_backoff_max = worker_config.poll_backoff_max
        self._poll_backoff_multiplier = worker_config.poll_backoff_multiplier
        self._poll_jitter = worker_config.poll_jitter
        self._current_poll_interval = worker_config.poll_interval
        self._max_concurrency = worker_config.max_concurrency
        self._reconcile_interval = worker_config.reconcile_interval
        self._stale_after = (
            timedelta(seconds=worker_config.stale_after) if worker_config.stale_after is not None else None
        )
        self._stale_check_interval = worker_config.stale_check_interval
        self._graceful_shutdown_timeout = worker_config.graceful_shutdown_timeout
        self._final_cancel_timeout = worker_config.final_cancel_timeout
        self._worker_id = worker_config.id if worker_config.id is not None else f"worker-{os.getpid()}"
        self._heartbeat_manager = WorkerHeartbeatManager(
            service,
            interval=worker_config.heartbeat_interval,
            miss_threshold=worker_config.heartbeat_miss_threshold,
            worker_id=self._worker_id,
        )
        self._queues = worker_config.queues
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

    def _reset_poll_backoff(self) -> "None":
        """Reset the adaptive polling wait to the base interval.

        Called on worker start, any claimed record, a native notification,
        and a recoverable backend/listener exception.
        """
        self._current_poll_interval = self._poll_interval

    def _advance_poll_backoff(self) -> "None":
        """Grow the adaptive polling wait after a fully empty poll/reconciliation cycle.

        A no-op while backoff is disabled (``poll_backoff_max`` is ``None``),
        preserving the fixed-interval path exactly.
        """
        if self._poll_backoff_max is None:
            return
        self._current_poll_interval = _next_backoff_interval(
            self._current_poll_interval,
            base=self._poll_interval,
            maximum=self._poll_backoff_max,
            multiplier=self._poll_backoff_multiplier,
        )

    async def _current_wait_timeout(self) -> "float":
        """Return the wait timeout for the next poll, with jitter applied when enabled.

        Jitter perturbs only the returned wait; :attr:`_current_poll_interval`
        (the stored exponential state) is never mutated by it. No random
        sampling occurs while backoff is disabled or jitter is zero.

        While backoff is enabled, the wait is additionally clamped to the
        backend's ``time_until_next_due()`` when known: no backend notifies
        a worker the instant a scheduled or retried record's due time
        arrives, so an uncapped backoff wait could otherwise sleep past
        already-known future work. This clamp never applies to the fixed
        (backoff-disabled) path, matching its exact prior behavior.

        Returns:
            The timeout, in seconds, to pass to ``wait_for_wakeups``.
        """
        if self._poll_backoff_max is None:
            return self._current_poll_interval
        timeout = (
            self._current_poll_interval
            if self._poll_jitter <= 0.0
            else _apply_jitter(
                self._current_poll_interval,
                base=self._poll_interval,
                maximum=self._poll_backoff_max,
                jitter=self._poll_jitter,
            )
        )
        due_in = await self._service.get_queue_backend().time_until_next_due(queues=self._queues)
        if due_in is not None and due_in < timeout:
            timeout = due_in
        return timeout

    async def start(self) -> "None":
        """Run the worker loop until stopped or cancelled."""
        self._is_running = True
        self._stop_event.clear()
        self._reset_poll_backoff()
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
                    self._reset_poll_backoff()
                    await self._backoff_after_loop_error()
                    continue
                if processed:
                    self._reset_poll_backoff()
                    continue
                outcome = await self._wait_for_work()
                if outcome:
                    self._reset_poll_backoff()
                elif outcome is False:
                    self._advance_poll_backoff()
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
        """Reconcile externally dispatched records by delegating to the service.

        The reconciliation contract (state transitions, unknown-backend
        skipping, and metrics) lives on :meth:`QueueService.reconcile_external`;
        the worker keeps only the periodic cadence and fleet lock.

        Returns:
            Number of records that reached a terminal queue status.
        """
        return await self._service.reconcile_external(limit=limit)

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
            await self.reconcile_external(limit=self._batch_size)
            return
        now = asyncio.get_running_loop().time()
        if now - self._last_reconcile_at < self._reconcile_interval:
            return
        self._last_reconcile_at = now
        await self.reconcile_external(limit=self._batch_size)

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

    async def _wait_for_work(self) -> "bool | None":
        """Wait for new work, a backend notification, or a stop signal.

        The adaptive polling wait (when enabled) is passed directly as the
        backend's wait timeout; no additional sleep follows a native wait.

        Returns:
            ``True`` when a native backend notification was observed (the
            caller resets the adaptive backoff to the base interval).
            ``False`` when the wait fully elapsed with no notification (the
            caller advances the backoff). ``None`` when the wait was
            interrupted by stop or a pending completion signal, in which case
            backoff state is left unchanged.
        """
        if self._completion_event.is_set():
            self._completion_event.clear()
            return None
        queue_backend = self._service.get_queue_backend()
        started_at = time.perf_counter()
        timeout = await self._current_wait_timeout()
        notification_task = asyncio.create_task(queue_backend.wait_for_wakeups(timeout=timeout))
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
        outcome: "bool | None" = None
        if notification_task in done:
            outcome = await self._consume_wait_task(notification_task)
        self._service.observability_runtime.record_duration(
            "litestar_queues.worker.idle.duration",
            time.perf_counter() - started_at,
            attributes={**self._worker_metric_base_attributes(), "worker.wakeup": str(notification_task in done)},
        )
        return outcome

    async def _consume_wait_task(self, task: "asyncio.Task[bool]") -> "bool | None":
        try:
            result = task.result()
        except asyncio.TimeoutError:
            return False
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # A native read failure must not kill the run loop; durable polling
            # (run_once) still discovers work, and the next wait re-establishes
            # the listener from a clean state. The backoff resets so a stale
            # listener does not compound into a longer discovery delay.
            self._record_counter("litestar_queues.worker.loop.error.count", {"worker.error.type": type(exc).__name__})
            logger.exception("Queue worker loop iteration failed", extra={"worker_id": self._worker_id})
            self._reset_poll_backoff()
            await self._backoff_after_loop_error()
            return None
        return result

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

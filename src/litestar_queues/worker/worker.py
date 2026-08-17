import asyncio
import contextlib
import logging
import os
import random
import time
from datetime import timedelta
from typing import TYPE_CHECKING

from litestar_queues.config import WorkerConfig, execution_backend_name, queue_backend_name
from litestar_queues.events import bind_beat_sink
from litestar_queues.events.context import _cancel_task_context
from litestar_queues.exceptions import QueueConfigurationError
from litestar_queues.worker.heartbeat import WorkerHeartbeatManager

if TYPE_CHECKING:
    from uuid import UUID

    from litestar_queues.models import QueuedTaskRecord
    from litestar_queues.service import QueueService

__all__ = ("Worker",)


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
        "_cancel_requested",
        "_cancellation_poll_interval",
        "_completion_event",
        "_current_poll_interval",
        "_expiry_check_interval",
        "_final_cancel_timeout",
        "_graceful_shutdown_timeout",
        "_heartbeat_manager",
        "_is_running",
        "_last_cancellation_check_at",
        "_last_expiry_check_at",
        "_last_reconcile_at",
        "_last_stale_check_at",
        "_listener_reconnect_pending",
        "_logger",
        "_max_concurrency",
        "_max_interruptions",
        "_poll_backoff_max",
        "_poll_backoff_multiplier",
        "_poll_interval",
        "_poll_jitter",
        "_queue_concurrency",
        "_queues",
        "_reconcile_interval",
        "_requeue_on_shutdown",
        "_running_tasks",
        "_service",
        "_stale_after",
        "_stale_check_interval",
        "_started_event",
        "_startup_completion",
        "_startup_error",
        "_stop_event",
        "_wakeup_started_at",
        "_worker_id",
    )

    def __init__(self, service: "QueueService", config: "WorkerConfig | None" = None) -> "None":
        """Initialize the worker.

        Args:
            service: Queue service used to reach the configured backends.
            config: Worker runtime configuration; ``None`` uses defaults.
        """
        worker_config = config or service.config.worker
        self._service = service
        self._logger = logging.getLogger(service.config.names.logger("worker"))
        self._batch_size = worker_config.batch_size
        self._cancellation_poll_interval = worker_config.cancellation_poll_interval
        self._poll_interval = worker_config.poll_interval
        self._poll_backoff_max = worker_config.poll_backoff_max
        self._poll_backoff_multiplier = worker_config.poll_backoff_multiplier
        self._poll_jitter = worker_config.poll_jitter
        self._current_poll_interval = worker_config.poll_interval
        self._max_concurrency = worker_config.max_concurrency
        self._queue_concurrency = dict(worker_config.queue_concurrency)
        self._reconcile_interval = worker_config.reconcile_interval
        self._requeue_on_shutdown = worker_config.requeue_on_shutdown
        self._max_interruptions = worker_config.max_interruptions
        self._expiry_check_interval = worker_config.expiry_check_interval
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
            jitter_fraction=worker_config.heartbeat_jitter_fraction,
            on_claim_lost=self._cancel_claim_lost_task if worker_config.cancel_on_claim_loss else None,
        )
        self._queues = worker_config.queues
        self._running_tasks: "dict[asyncio.Task[None], QueuedTaskRecord]" = {}
        self._cancel_requested: "set[asyncio.Task[None]]" = set()
        self._stop_event = asyncio.Event()
        self._completion_event = asyncio.Event()
        self._startup_completion: "asyncio.Future[BaseException | None] | None" = None
        self._started_event = asyncio.Event()
        self._startup_error: "BaseException | None" = None
        self._is_running = False
        self._last_reconcile_at = -float("inf")
        self._last_expiry_check_at = -float("inf")
        self._last_cancellation_check_at = -float("inf")
        self._last_stale_check_at = -float("inf")
        self._listener_reconnect_pending = False
        self._wakeup_started_at: "float | None" = None

    @property
    def is_running(self) -> "bool":
        """Whether the worker loop is active."""
        return self._is_running

    @property
    def worker_id(self) -> "str":
        """Worker identity used for events and logs."""
        return self._worker_id

    async def wait_started(self) -> "None":
        """Wait until heartbeat startup succeeds or propagate its failure."""
        startup_completion = self._startup_completion
        if startup_completion is not None and startup_completion.done():
            await asyncio.sleep(0)
            startup_completion = self._startup_completion
        if startup_completion is None:
            startup_completion = asyncio.get_running_loop().create_future()
            self._startup_completion = startup_completion
        startup_error = await asyncio.shield(startup_completion)
        if startup_error is not None:
            raise startup_error

    def _prepare_startup(self) -> "asyncio.Future[BaseException | None]":
        """Create or adopt the completion shared by this startup generation."""
        startup_completion = self._startup_completion
        if startup_completion is None or startup_completion.done():
            startup_completion = asyncio.get_running_loop().create_future()
            self._startup_completion = startup_completion
        self._started_event.clear()
        self._startup_error = None
        return startup_completion

    def _complete_startup(
        self, startup_completion: "asyncio.Future[BaseException | None]", startup_error: "BaseException | None"
    ) -> "None":
        """Publish one startup generation's immutable outcome to its waiters."""
        self._startup_error = startup_error
        self._started_event.set()
        startup_completion.set_result(startup_error)

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
        startup_completion = self._prepare_startup()
        self._is_running = True
        self._stop_event.clear()
        self._reset_poll_backoff()
        try:
            try:
                await self._heartbeat_manager.start()
            except BaseException as exc:
                self._complete_startup(startup_completion, exc)
                raise
            self._complete_startup(startup_completion, None)
            while not self._stop_event.is_set():
                try:
                    await self._maybe_expire_overdue()
                    await self._maybe_requeue_stale()
                    await self._maybe_reconcile_external()
                    processed = await self.run_once()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self._record_counter("litestar_queues.worker.loop.error", {"worker.error.type": type(exc).__name__})
                    self._logger.exception("Queue worker loop iteration failed", extra={"worker_id": self._worker_id})
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

        Raises:
            QueueConfigurationError: If the execution backend schedules its own
                delivery, leaving a worker nothing it may safely claim.
        """
        await self._maybe_cancel_running()
        execution_backend = self._service.get_execution_backend()
        if execution_backend.schedules_on_enqueue:
            msg = (
                f"execution_backend={execution_backend_name(self._service.config.execution_backend)!r} "
                f"schedules delivery when a record is persisted, so a worker would dispatch it a "
                f"second time. Run this deployment with no worker process."
            )
            raise QueueConfigurationError(msg)
        available = min(self._batch_size, max(0, self._max_concurrency - len(self._running_tasks)))
        if available <= 0:
            return 0
        if execution_backend.is_external:
            records = await self._list_pending(limit=available)
            dispatched = await self._dispatch_external(records)
            if not dispatched:
                self._record_empty_poll()
            return dispatched

        claimed_records = await self._claim_available(limit=available)
        if not claimed_records:
            self._record_empty_poll()
            return 0

        self._record_claimed(claimed_records)
        for record in claimed_records:
            self._track_execution(record)
        return len(claimed_records)

    async def _claim_available(self, *, limit: "int") -> "list[QueuedTaskRecord]":
        execution_backend_name_ = execution_backend_name(self._service.config.execution_backend)
        active_by_queue: "dict[str, int]" = {}
        for record in self._running_tasks.values():
            active_by_queue[record.queue] = active_by_queue.get(record.queue, 0) + 1
        queue_limits = {
            queue: max(0, cap - active_by_queue.get(queue, 0)) for queue, cap in self._queue_concurrency.items()
        }
        claimed = await self._service.claim_tasks(
            limit=limit,
            queues=self._queues,
            execution_backend=execution_backend_name_,
            worker_id=self._worker_id,
            queue_limits=queue_limits or None,
        )
        owned: "list[QueuedTaskRecord]" = []
        backend = self._service.get_queue_backend()
        for record in claimed:
            assigned = await backend.assign_worker(
                record.id, worker_id=self._worker_id, expected_retry_count=record.retry_count
            )
            if assigned is not None:
                owned.append(assigned)
        claimed = owned
        wakeup_started_at = self._wakeup_started_at
        self._wakeup_started_at = None
        if claimed and wakeup_started_at is not None:
            self._service.observability_runtime.record_duration(
                "litestar_queues.worker.wakeup_to_claim.duration",
                time.perf_counter() - wakeup_started_at,
                attributes=self._transport_metric_attributes(),
            )
        return claimed

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
        self._running_tasks[task] = record
        task.add_done_callback(self._on_execution_done)
        return task

    def _on_execution_done(self, task: "asyncio.Task[None]") -> "None":
        self._running_tasks.pop(task, None)
        self._cancel_requested.discard(task)
        # Retrieve the outcome here rather than through a drain-time gather, so
        # a task that finishes outside a drain never trips asyncio's
        # "exception was never retrieved" reporting.
        if not task.cancelled():
            error = task.exception()
            if error is not None:
                self._logger.error(
                    "Queue task execution failed outside its own error handling",
                    exc_info=error,
                    extra={"worker_id": self._worker_id},
                )
        self._completion_event.set()

    def _cancel_claim_lost_task(self, task_id: "UUID") -> "None":
        """Cancel the local coroutine whose claim the heartbeat manager just lost.

        Deliberately a plain ``cancel()`` rather than the durable-cancel pair:
        claim loss is an ownership interruption, not a user cancel, so the
        attempt records ``interrupted`` telemetry and attempts no terminal
        write. The record already belongs to whichever worker took the claim.
        """
        for task, record in self._running_tasks.items():
            if record.id != task_id or task.done():
                continue
            task.cancel()
            self._record_counter(
                "litestar_queues.worker.claim_lost_cancel", {"messaging.destination.name": record.queue}
            )
            return

    async def _maybe_cancel_running(self) -> "None":
        if not self._running_tasks:
            return
        now = asyncio.get_running_loop().time()
        if now - self._last_cancellation_check_at < self._cancellation_poll_interval:
            return
        self._last_cancellation_check_at = now
        by_id = {record.id: task for task, record in self._running_tasks.items()}
        records = await self._service.get_queue_backend().get_tasks(tuple(by_id))
        for record in records:
            if record.status != "cancelled":
                continue
            task = by_id.get(record.id)
            if task is None or task.done():
                continue
            _cancel_task_context(str(record.id))
            task.cancel()

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
        try:
            with bind_beat_sink(self._heartbeat_manager):
                try:
                    await self._service.get_execution_backend().execute(
                        self._service, record, worker_id=self._worker_id
                    )
                except asyncio.CancelledError:
                    task_setting = record.metadata.get("requeue_on_shutdown")
                    should_requeue = self._requeue_on_shutdown if task_setting is None else task_setting is True
                    if self._stop_event.is_set() and should_requeue:
                        # A backend failure here must never replace the cancellation:
                        # the exception would be swallowed whole by the drain/cancel
                        # wait and the record would silently stay `running`.
                        try:
                            updated = await self._service.interrupt_task(
                                record, worker_id=self._worker_id, max_interruptions=self._max_interruptions
                            )
                        except Exception:
                            self._record_counter(
                                "litestar_queues.worker.interrupt.error", {"messaging.destination.name": record.queue}
                            )
                            self._logger.exception(
                                "Queue task shutdown requeue failed; record remains running",
                                extra={"worker_id": self._worker_id, "task_id": str(record.id)},
                            )
                        else:
                            if updated is None:
                                self._logger.warning(
                                    "Queue task shutdown requeue lost its fence",
                                    extra={"worker_id": self._worker_id, "task_id": str(record.id)},
                                )
                    raise
        finally:
            try:
                try:
                    self._heartbeat_manager.unregister(record.id)
                except Exception as exc:  # noqa: BLE001 - heartbeat cleanup must not skip backend clearing.
                    self._record_heartbeat_failure(exc, "Queue task heartbeat cleanup failed")
            finally:
                await self._close_heartbeat_manager_if_idle()

    async def _dispatch_external(self, records: "list[QueuedTaskRecord]") -> "int":
        # The execution backend owns litestar_queues.execution.dispatch: it is
        # the only layer that can distinguish a fallback from an outright failure,
        # and a second emitter here would double-count every dispatch.
        execution_backend = self._service.get_execution_backend()
        dispatched = 0
        for record in records:
            if record.execution_ref is not None:
                continue
            if record.is_expired:
                continue
            if await execution_backend.dispatch(self._service, record) is not None:
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
        # One bounded outcome label, each incremented by its own count. Recording the
        # counts as label *values* would mint a new time series per distinct tally.
        outcomes = (
            ("requeued", result.requeued),
            ("failed", result.failed),
            ("skipped", result.skipped),
            ("handler_needed", result.handler_needed),
        )
        for outcome, count in outcomes:
            if count:
                self._record_counter("litestar_queues.stale_recovery", {"queue.stale.outcome": outcome}, value=count)

    async def _maybe_expire_overdue(self) -> "None":
        interval = self._expiry_check_interval
        if interval is None:
            return
        now = asyncio.get_running_loop().time()
        if now - self._last_expiry_check_at < interval:
            return
        self._last_expiry_check_at = now
        if not await self._service.get_queue_backend().acquire_worker_lock(
            "expiry_sweep", ttl=timedelta(seconds=max(interval, 1.0))
        ):
            return
        expired = await self._service.expire_overdue_tasks(worker_id=self._worker_id)
        if expired:
            self._record_counter("litestar_queues.expiry", {"queue.expiry.outcome": "expired"}, value=len(expired))

    async def _maybe_reconcile_external(self) -> "None":
        # Check the local cadence before the fleet lock, matching the stale and
        # expiry passes. Taking the lock first made every worker loop iteration
        # write a coordination record only to discard it at the interval check.
        if self._reconcile_interval > 0:
            now = asyncio.get_running_loop().time()
            if now - self._last_reconcile_at < self._reconcile_interval:
                return
            self._last_reconcile_at = now
        if not await self._service.get_queue_backend().acquire_worker_lock(
            "external_reconcile", ttl=timedelta(seconds=max(self._reconcile_interval, 1.0))
        ):
            return
        await self.reconcile_external(limit=self._batch_size)

    async def _drain_running(self) -> "bool":
        if not self._running_tasks:
            return False
        # `asyncio.wait`, not `wait_for(gather(...))`: a timed-out `wait_for`
        # cancels the gather, and that cancellation propagates into every
        # execution task as an unaccounted extra delivery.
        _, pending = await asyncio.wait(tuple(self._running_tasks), timeout=self._graceful_shutdown_timeout)
        if pending:
            await self._cancel_running()
            return True
        return False

    async def _cancel_running(self) -> "None":
        tasks = tuple(self._running_tasks)
        if not tasks:
            return
        # `stop()` and the loop's own shutdown drain both land here. Cancelling
        # a task twice delivers a second CancelledError into the task's own
        # shutdown-requeue handler and aborts the backend write mid-flight, so
        # each execution task is cancelled exactly once per worker.
        for task in tasks:
            if task not in self._cancel_requested:
                self._cancel_requested.add(task)
                task.cancel()
        await asyncio.wait(tasks, timeout=self._final_cancel_timeout)
        await self._hand_off_undead_tasks()

    async def _hand_off_undead_tasks(self) -> "None":
        """Null the heartbeats of tasks that outlived their cancellation budget.

        The records stay ``running`` and this worker no longer touches their
        heartbeats, so a stale sweep (``stale_after``) can reclaim them on its
        next pass instead of waiting out a full heartbeat age.
        """
        survivors = [record for task, record in self._running_tasks.items() if not task.done()]
        if not survivors:
            return
        self._logger.warning(
            "Queue tasks survived cancellation; abandoning them to stale recovery",
            extra={"worker_id": self._worker_id, "task_ids": [str(record.id) for record in survivors]},
        )
        # `null_heartbeats` carries one fence value per call, so group the
        # survivors by the generation this worker still believes it owns.
        by_generation: "dict[int, list[UUID]]" = {}
        for record in survivors:
            by_generation.setdefault(record.retry_count, []).append(record.id)
        for expected_retry_count, task_ids in by_generation.items():
            await self._null_heartbeats_quietly(task_ids, expected_retry_count=expected_retry_count)

    async def _null_heartbeats_quietly(self, task_ids: "list[UUID]", *, expected_retry_count: "int") -> "None":
        """Clear one generation's heartbeats, reporting rather than raising a backend failure."""
        try:
            await self._service.get_queue_backend().null_heartbeats(task_ids, expected_retry_count=expected_retry_count)
        except Exception:
            self._logger.exception(
                "Nulling heartbeats for surviving queue tasks failed", extra={"worker_id": self._worker_id}
            )

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
        wait_attributes = {"queue.backend": self._queue_backend_name(), "worker.wait.kind": self._wait_kind()}
        self._service.observability_runtime.record_histogram(
            "litestar_queues.worker.poll.delay", timeout, unit="s", attributes=wait_attributes
        )
        if self._listener_reconnect_pending:
            self._service.observability_runtime.record_counter(
                "litestar_queues.listener.reconnect", attributes=self._transport_metric_attributes()
            )
            self._listener_reconnect_pending = False
        notification_task = asyncio.create_task(queue_backend.wait_for_wakeups(timeout=timeout))
        control_task = asyncio.create_task(
            queue_backend.wait_for_worker_control(worker_id=self._worker_id, timeout=timeout)
        )
        stop_task = asyncio.create_task(self._stop_event.wait())
        completion_task = asyncio.create_task(self._completion_event.wait())
        done, pending = await asyncio.wait(
            {notification_task, control_task, stop_task, completion_task}, return_when=asyncio.FIRST_COMPLETED
        )
        self._completion_event.clear()
        for task in pending:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        if control_task in done and await self._consume_control_task(control_task):
            # A saturated worker never reaches run_once's cadence check quickly
            # enough for the hint to matter, so the gate is reset before the pass.
            self._last_cancellation_check_at = -float("inf")
            await self._maybe_cancel_running()
        outcome: "bool | None" = None
        if notification_task in done:
            outcome = await self._consume_wait_task(notification_task)
            if outcome:
                self._wakeup_started_at = time.perf_counter()
        elapsed = time.perf_counter() - started_at
        self._service.observability_runtime.record_duration(
            "litestar_queues.worker.wait.duration", elapsed, attributes=wait_attributes
        )
        self._service.observability_runtime.record_duration(
            "litestar_queues.worker.idle.duration",
            elapsed,
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
            self._record_counter("litestar_queues.worker.loop.error", {"worker.error.type": type(exc).__name__})
            self._service.observability_runtime.record_counter(
                "litestar_queues.listener.error",
                attributes={**self._transport_metric_attributes(), "queue.outcome": "read_failed"},
            )
            self._listener_reconnect_pending = True
            self._logger.exception("Queue worker loop iteration failed", extra={"worker_id": self._worker_id})
            self._reset_poll_backoff()
            await self._backoff_after_loop_error()
            return None
        return result

    async def _consume_control_task(self, task: "asyncio.Task[bool]") -> "bool":
        """Return whether a worker-control hint arrived, containing read failures.

        Returns:
            True when a hint was observed and an immediate cancellation pass
            should run.
        """
        try:
            return task.result()
        except asyncio.TimeoutError:
            return False
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # Control hints are lossy by contract: a failed read costs latency,
            # never correctness, because run_once still polls durable status.
            self._record_counter("litestar_queues.worker.loop.error", {"worker.error.type": type(exc).__name__})
            self._service.observability_runtime.record_counter(
                "litestar_queues.listener.error",
                attributes={**self._transport_metric_attributes(), "queue.outcome": "control_read_failed"},
            )
            self._listener_reconnect_pending = True
            self._logger.exception("Queue worker loop iteration failed", extra={"worker_id": self._worker_id})
            return False

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
        # Delegated so both emitters of litestar_queues.heartbeat.failure agree
        # on one label set; divergent label names break the Prometheus collector.
        self._heartbeat_manager.record_failure(exc, message)

    def _record_claimed(self, records: "list[QueuedTaskRecord]") -> "None":
        counts: "dict[str, int]" = {}
        for record in records:
            counts[record.queue] = counts.get(record.queue, 0) + 1
        for queue, count in counts.items():
            self._record_counter("litestar_queues.worker.claim", {"messaging.destination.name": queue}, value=count)

    def _record_empty_poll(self) -> "None":
        self._service.observability_runtime.record_counter(
            "litestar_queues.worker.poll.empty", attributes={"queue.backend": self._queue_backend_name()}
        )

    def _queue_backend_name(self) -> "str":
        return queue_backend_name(self._service.config.queue_backend)

    def _transport_metric_attributes(self) -> "dict[str, str]":
        capabilities = self._service.get_queue_backend().capabilities
        return {
            "queue.backend": self._queue_backend_name(),
            "queue.transport": capabilities.wakeup_backend or "polling",
        }

    def _wait_kind(self) -> "str":
        return "native" if self._service.get_queue_backend().capabilities.supports_worker_wakeups else "polling"

    def _record_counter(self, name: "str", attributes: "dict[str, str]", *, value: "int" = 1) -> "None":
        self._service.observability_runtime.record_counter(
            name, value, attributes={**self._worker_metric_base_attributes(), **attributes}
        )

    def _worker_metric_base_attributes(self) -> "dict[str, str]":
        return {"queue.execution.backend": execution_backend_name(self._service.config.execution_backend)}

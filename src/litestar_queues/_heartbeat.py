import asyncio
import contextlib
import logging
import random
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING
from uuid import UUID

from litestar_queues.models import HeartbeatTouch

if TYPE_CHECKING:
    from litestar_queues.service import QueueService

__all__ = ("WorkerHeartbeatManager",)

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class _HeartbeatRegistration:
    expected_retry_count: "int | None"
    consecutive_misses: "int" = 0


class WorkerHeartbeatManager:
    """Worker-owned bulk heartbeat ticker."""

    __slots__ = (
        "_beats",
        "_interval",
        "_jitter_fraction",
        "_miss_threshold",
        "_registrations",
        "_service",
        "_stop_event",
        "_task",
        "_worker_id",
    )

    def __init__(
        self,
        service: "QueueService",
        *,
        interval: "float",
        miss_threshold: "int" = 2,
        worker_id: "str",
        jitter_fraction: "float" = 0.1,
    ) -> "None":
        self._service = service
        self._interval = interval
        self._miss_threshold = max(1, miss_threshold)
        self._worker_id = worker_id
        self._jitter_fraction = max(0.0, jitter_fraction)
        self._registrations: "dict[UUID, _HeartbeatRegistration]" = {}
        self._beats: "dict[UUID, str | None]" = {}
        self._stop_event = asyncio.Event()
        self._task: "asyncio.Task[None] | None" = None

    @property
    def has_registrations(self) -> "bool":
        """Whether any tasks are currently registered for heartbeat ticks."""
        return bool(self._registrations)

    def register(self, task_id: "UUID", *, expected_retry_count: "int | None") -> "None":
        """Register a running task for bulk heartbeat ticks."""
        is_new = task_id not in self._registrations
        self._registrations[task_id] = _HeartbeatRegistration(expected_retry_count=expected_retry_count)
        if is_new:
            self._record_gauge_delta("litestar_queues.heartbeat.active", 1)

    def unregister(self, task_id: "UUID") -> "None":
        """Remove a task from heartbeat ticks.

        Returns:
            None.
        """
        if task_id not in self._registrations:
            return
        del self._registrations[task_id]
        self._beats.pop(task_id, None)
        self._record_gauge_delta("litestar_queues.heartbeat.active", -1)

    def record_beat(self, task_id: "str", detail: "str | None") -> "None":
        """Store the latest progress detail for a registered task.

        Returns:
            None.
        """
        with contextlib.suppress(ValueError):
            parsed_task_id = UUID(task_id)
            if parsed_task_id in self._registrations:
                self._beats[parsed_task_id] = detail[:256] if isinstance(detail, str) else None

    async def start(self) -> "None":
        """Start the background heartbeat loop once.

        Returns:
            None.
        """
        if self._task is not None and not self._task.done():
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run())

    async def aclose(self) -> "None":
        """Stop the loop, flush current registrations, and clear local state."""
        self._stop_event.set()
        task = self._task
        if task is not None and not task.done():
            with contextlib.suppress(asyncio.CancelledError):
                await task
        await self._tick()
        for task_id in tuple(self._registrations):
            self.unregister(task_id)
        self._beats.clear()

    async def _tick(self) -> "None":
        registrations = dict(self._registrations)
        if not registrations:
            return

        touches = []
        for task_id, registration in registrations.items():
            detail = self._beats.get(task_id)
            touches.append(
                HeartbeatTouch(
                    task_id=task_id,
                    expected_retry_count=registration.expected_retry_count,
                    metadata_patch={"progress_detail": detail} if detail else None,
                )
            )
        started_at = time.perf_counter()
        try:
            result = await self._service.get_queue_backend().touch_heartbeats(touches)
        except Exception as exc:  # noqa: BLE001 - backend failures must not kill heartbeat loops.
            self._record_failure(exc)
            return

        attributes = self._metric_attributes()
        self._service.observability_runtime.record_duration(
            "litestar_queues.heartbeat.flush.duration", time.perf_counter() - started_at, attributes=attributes
        )
        self._service.observability_runtime.record_counter(
            "litestar_queues.heartbeat.flush.count", len(result.touched_task_ids), attributes=attributes
        )

        for task_id in result.touched_task_ids:
            touched_registration = self._registrations.get(task_id)
            if touched_registration is not None:
                touched_registration.consecutive_misses = 0
                self._beats.pop(task_id, None)

        for task_id in result.missed_task_ids:
            missed_registration = self._registrations.get(task_id)
            if missed_registration is None:
                continue
            missed_registration.consecutive_misses += 1
            self._service.observability_runtime.record_counter(
                "litestar_queues.heartbeat.missed.count", 1, attributes=attributes
            )
            if missed_registration.consecutive_misses >= self._miss_threshold:
                await self._publish_claim_lost(task_id, missed_registration)

    async def _run(self) -> "None":
        while not self._stop_event.is_set():
            if await self._wait_for_next_tick():
                await self._safe_tick()

    async def _publish_claim_lost(self, task_id: "UUID", registration: "_HeartbeatRegistration") -> "None":
        try:
            record = await self._service.get_queue_backend().get_task(task_id)
            if record is not None:
                await self._service.publish_claim_lost(
                    record,
                    phase="heartbeat",
                    worker_id=self._worker_id,
                    expected_retry_count=registration.expected_retry_count,
                )
        except Exception as exc:  # noqa: BLE001 - backend/event failures are reported and contained.
            self._record_failure(exc)
            return
        self.unregister(task_id)

    async def _safe_tick(self) -> "None":
        try:
            await self._tick()
        except Exception as exc:  # noqa: BLE001 - defensive loop guard.
            self._record_failure(exc)

    async def _wait_for_next_tick(self) -> "bool":
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=self._sleep_interval())
        except asyncio.TimeoutError:
            return True
        return False

    def _sleep_interval(self) -> "float":
        jitter = random.uniform(0, self._jitter_fraction) if self._jitter_fraction else 0  # noqa: S311
        return max(0.0, self._interval * (1 + jitter))

    def _metric_attributes(self) -> "dict[str, str]":
        return {"queue.backend": type(self._service.get_queue_backend()).__name__}

    def _record_failure(self, exc: "BaseException") -> "None":
        self._service.observability_runtime.record_counter(
            "litestar_queues.heartbeat.failure.count",
            1,
            attributes={**self._metric_attributes(), "worker.error.type": type(exc).__name__},
        )
        logger.warning(
            "Queue worker heartbeat tick failed",
            exc_info=(type(exc), exc, exc.__traceback__),
            extra={"worker_id": self._worker_id},
        )

    def _record_gauge_delta(self, name: "str", delta: "int") -> "None":
        self._service.observability_runtime.record_gauge_delta(name, delta, attributes=self._metric_attributes())

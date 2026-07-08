import asyncio
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

import pytest

from litestar_queues._heartbeat import WorkerHeartbeatManager
from litestar_queues.models import HeartbeatTouch, HeartbeatTouchResult, QueuedTaskRecord

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

pytestmark = pytest.mark.anyio


async def test_one_touch_call_per_tick_for_many_tasks() -> "None":
    """A tick should issue one backend bulk call for all registered tasks."""
    backend = FakeBackend()
    service = FakeService(backend)
    manager = WorkerHeartbeatManager(service, interval=30, worker_id="worker-a", jitter_fraction=0)
    task_ids = [uuid4() for _ in range(1000)]

    for task_id in task_ids:
        manager.register(task_id, expected_retry_count=1)
    await manager._tick()

    assert len(backend.touch_calls) == 1
    assert len(backend.touch_calls[0]) == 1000
    assert {touch.task_id for touch in backend.touch_calls[0]} == set(task_ids)
    assert service.observability_runtime.counters == [
        ("litestar_queues.heartbeat.flush.count", 1000, {"queue.backend": "FakeBackend"})
    ]
    assert service.observability_runtime.durations[0][0] == "litestar_queues.heartbeat.flush.duration"


async def test_single_miss_does_not_claim_lost() -> "None":
    """The first missed heartbeat should be retained for the next tick."""
    task_id = uuid4()
    backend = FakeBackend()
    backend.touch_results.append(HeartbeatTouchResult(missed_task_ids={task_id}))
    service = FakeService(backend)
    manager = WorkerHeartbeatManager(service, interval=30, worker_id="worker-a", jitter_fraction=0)

    manager.register(task_id, expected_retry_count=3)
    await manager._tick()

    assert service.claim_lost_calls == []
    assert task_id in manager._registrations
    assert manager._registrations[task_id].consecutive_misses == 1
    assert (
        "litestar_queues.heartbeat.flush.count",
        0,
        {"queue.backend": "FakeBackend"},
    ) in service.observability_runtime.counters


async def test_second_consecutive_miss_claims_lost() -> "None":
    """The second consecutive missed heartbeat should publish claim-lost and unregister."""
    task_id = uuid4()
    record = QueuedTaskRecord(task_name="heartbeat.missed", id=task_id, retry_count=5)
    backend = FakeBackend(records={task_id: record})
    backend.touch_results.extend([
        HeartbeatTouchResult(missed_task_ids={task_id}),
        HeartbeatTouchResult(missed_task_ids={task_id}),
    ])
    service = FakeService(backend)
    manager = WorkerHeartbeatManager(service, interval=30, worker_id="worker-a", jitter_fraction=0)

    manager.register(task_id, expected_retry_count=5)
    await manager._tick()
    await manager._tick()

    assert service.claim_lost_calls == [(record, "heartbeat", "worker-a", 5)]
    assert backend.get_task_calls == [task_id]
    assert task_id not in manager._registrations


async def test_shutdown_flush_and_clear() -> "None":
    """Closing should final-flush current registrations and leave no loop task running."""
    task_id = uuid4()
    backend = FakeBackend()
    service = FakeService(backend)
    manager = WorkerHeartbeatManager(service, interval=60, worker_id="worker-a", jitter_fraction=0)

    manager.register(task_id, expected_retry_count=1)
    manager.record_beat(str(task_id), "current detail")
    await manager.start()
    loop_task = manager._task
    assert loop_task is not None
    assert loop_task.done() is False

    await manager.aclose()

    assert len(backend.touch_calls) == 1
    assert backend.touch_calls[0][0].task_id == task_id
    assert manager._registrations == {}
    assert manager._beats == {}
    assert loop_task.done() is True


async def test_aclose_waits_for_in_flight_tick_before_final_flush() -> "None":
    """Closing should not cancel an in-flight backend heartbeat write."""
    task_id = uuid4()
    backend = BlockingBackend()
    service = FakeService(backend)
    manager = WorkerHeartbeatManager(service, interval=0.001, worker_id="worker-a", jitter_fraction=0)

    manager.register(task_id, expected_retry_count=1)
    await manager.start()
    await asyncio.wait_for(backend.touch_started.wait(), timeout=1)

    close_task = asyncio.create_task(manager.aclose())
    await asyncio.sleep(0)

    assert close_task.done() is False
    assert backend.cancelled is False

    backend.release_touch.set()
    await asyncio.wait_for(close_task, timeout=1)

    assert backend.cancelled is False
    assert len(backend.touch_calls) == 2
    assert manager._registrations == {}


async def test_metrics_use_bounded_labels() -> "None":
    """Metrics should not include task ids, and active gauge deltas should balance."""
    task_id = uuid4()
    backend = FakeBackend()
    backend.touch_results.append(HeartbeatTouchResult(missed_task_ids={task_id}))
    service = FakeService(backend)
    manager = WorkerHeartbeatManager(service, interval=30, worker_id="worker-a", jitter_fraction=0)

    manager.register(task_id, expected_retry_count=1)
    await manager._tick()
    manager.unregister(task_id)

    metric_samples = [
        *service.observability_runtime.counters,
        *service.observability_runtime.durations,
        *service.observability_runtime.gauges,
    ]
    for _name, _value, attributes in metric_samples:
        assert "task_id" not in attributes
        assert str(task_id) not in attributes.values()
    active_deltas = [
        delta
        for name, delta, _attributes in service.observability_runtime.gauges
        if name == "litestar_queues.heartbeat.active"
    ]
    assert active_deltas == [1, -1]
    assert sum(active_deltas) == 0


async def test_record_beat_is_noop_safe_and_caps_detail() -> "None":
    """Beat details should only store for registered tasks and cap at 256 chars."""
    task_id = uuid4()
    backend = FakeBackend()
    service = FakeService(backend)
    manager = WorkerHeartbeatManager(service, interval=30, worker_id="worker-a", jitter_fraction=0)

    manager.record_beat(str(task_id), "ignored")
    manager.record_beat("not-a-uuid", "ignored")
    assert manager._beats == {}

    manager.register(task_id, expected_retry_count=1)
    manager.record_beat(str(task_id), "x" * 300)

    assert manager._beats == {task_id: "x" * 256}


async def test_last_value_wins_before_tick() -> "None":
    """Only the latest beat before a tick should be delivered."""
    task_id = uuid4()
    backend = FakeBackend()
    service = FakeService(backend)
    manager = WorkerHeartbeatManager(service, interval=30, worker_id="worker-a", jitter_fraction=0)

    manager.register(task_id, expected_retry_count=1)
    manager.record_beat(str(task_id), "row 1")
    manager.record_beat(str(task_id), "row 2")
    await manager._tick()

    assert backend.touch_calls[0][0].metadata_patch == {"progress_detail": "row 2"}


async def test_beat_cap_256_delivered_in_metadata_patch() -> "None":
    """Beat details should be capped before delivery."""
    task_id = uuid4()
    backend = FakeBackend()
    service = FakeService(backend)
    manager = WorkerHeartbeatManager(service, interval=30, worker_id="worker-a", jitter_fraction=0)

    manager.register(task_id, expected_retry_count=1)
    manager.record_beat(str(task_id), "x" * 500)
    await manager._tick()

    assert backend.touch_calls[0][0].metadata_patch == {"progress_detail": "x" * 256}


async def test_beat_cleared_after_successful_touch() -> "None":
    """Delivered beats should be cleared after a successful touch."""
    task_id = uuid4()
    backend = FakeBackend()
    service = FakeService(backend)
    manager = WorkerHeartbeatManager(service, interval=30, worker_id="worker-a", jitter_fraction=0)

    manager.register(task_id, expected_retry_count=1)
    manager.record_beat(str(task_id), "row 1")
    await manager._tick()
    await manager._tick()

    assert backend.touch_calls[0][0].metadata_patch == {"progress_detail": "row 1"}
    assert backend.touch_calls[1][0].metadata_patch is None


async def test_beat_retained_when_missed() -> "None":
    """Missed touches should keep the latest beat for the next tick."""
    task_id = uuid4()
    backend = FakeBackend()
    backend.touch_results.extend([
        HeartbeatTouchResult(missed_task_ids={task_id}),
        HeartbeatTouchResult(touched_task_ids={task_id}),
    ])
    service = FakeService(backend)
    manager = WorkerHeartbeatManager(service, interval=30, worker_id="worker-a", jitter_fraction=0)

    manager.register(task_id, expected_retry_count=1)
    manager.record_beat(str(task_id), "row 1")
    await manager._tick()
    await manager._tick()

    assert backend.touch_calls[0][0].metadata_patch == {"progress_detail": "row 1"}
    assert backend.touch_calls[1][0].metadata_patch == {"progress_detail": "row 1"}
    assert manager._beats == {}


def test_no_taskgroup_in_manager() -> "None":
    """The manager must stay Python 3.10-compatible."""
    manager_text = Path("src/litestar_queues/_heartbeat.py").read_text()

    assert "TaskGroup" not in manager_text


class FakeBackend:
    __slots__ = ("get_task_calls", "records", "touch_calls", "touch_results")

    def __init__(self, *, records: "Mapping[UUID, QueuedTaskRecord] | None" = None) -> "None":
        self.records = dict(records or {})
        self.touch_results: "list[HeartbeatTouchResult]" = []
        self.touch_calls: "list[list[HeartbeatTouch]]" = []
        self.get_task_calls: "list[UUID]" = []

    async def touch_heartbeats(self, touches: "Sequence[HeartbeatTouch]") -> "HeartbeatTouchResult":
        self.touch_calls.append(list(touches))
        if self.touch_results:
            return self.touch_results.pop(0)
        return HeartbeatTouchResult(touched_task_ids={touch.task_id for touch in touches})

    async def get_task(self, task_id: "UUID") -> "QueuedTaskRecord | None":
        self.get_task_calls.append(task_id)
        return self.records.get(task_id)


class BlockingBackend(FakeBackend):
    __slots__ = ("cancelled", "release_touch", "touch_started")

    def __init__(self) -> "None":
        super().__init__()
        self.touch_started = asyncio.Event()
        self.release_touch = asyncio.Event()
        self.cancelled = False

    async def touch_heartbeats(self, touches: "Sequence[HeartbeatTouch]") -> "HeartbeatTouchResult":
        self.touch_calls.append(list(touches))
        if len(self.touch_calls) == 1:
            self.touch_started.set()
            try:
                await self.release_touch.wait()
            except asyncio.CancelledError:
                self.cancelled = True
                raise
        return HeartbeatTouchResult(touched_task_ids={touch.task_id for touch in touches})


class FakeService:
    __slots__ = ("backend", "claim_lost_calls", "observability_runtime")

    def __init__(self, backend: "FakeBackend") -> "None":
        self.backend = backend
        self.observability_runtime = FakeObservabilityRuntime()
        self.claim_lost_calls: "list[tuple[QueuedTaskRecord, str, str, int | None]]" = []

    def get_queue_backend(self) -> "FakeBackend":
        return self.backend

    async def publish_claim_lost(
        self, record: "QueuedTaskRecord", *, phase: "str", worker_id: "str", expected_retry_count: "int | None"
    ) -> "None":
        self.claim_lost_calls.append((record, phase, worker_id, expected_retry_count))


class FakeObservabilityRuntime:
    __slots__ = ("counters", "durations", "enabled", "gauges")

    def __init__(self) -> "None":
        self.enabled = True
        self.counters: "list[tuple[str, int, dict[str, str]]]" = []
        self.durations: "list[tuple[str, float, dict[str, str]]]" = []
        self.gauges: "list[tuple[str, int, dict[str, str]]]" = []

    def record_counter(self, name: "str", value: "int" = 1, *, attributes: "Mapping[str, str]") -> "None":
        self.counters.append((name, value, dict(attributes)))

    def record_duration(self, name: "str", seconds: "float", *, attributes: "Mapping[str, str]") -> "None":
        self.durations.append((name, seconds, dict(attributes)))

    def record_gauge_delta(self, name: "str", delta: "int" = 1, *, attributes: "Mapping[str, str]") -> "None":
        self.gauges.append((name, delta, dict(attributes)))

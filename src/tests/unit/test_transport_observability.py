import asyncio
from typing import TYPE_CHECKING, Any

import pytest

from litestar_queues import QueueConfig, QueueService, TaskRequest, Worker, WorkerConfig
from litestar_queues.backends import InMemoryQueueBackend
from litestar_queues.events import EventBufferConfig, QueueEvent, QueueEventPublisher

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

pytestmark = pytest.mark.anyio


class _RecordingRuntime:
    enabled = True
    sqlcommenter_enabled = False

    def __init__(self) -> "None":
        self.counters: "list[tuple[str, int, dict[str, str]]]" = []
        self.durations: "list[tuple[str, float, dict[str, str]]]" = []
        self.histograms: "list[tuple[str, float, str, dict[str, str]]]" = []

    def start_span(
        self, name: "str", *, kind: "str", attributes: "Mapping[str, object]", parent: "object | None" = None
    ) -> "None":
        del name, kind, attributes, parent

    def end_span(self, span: "Any | None") -> "None":
        del span

    def record_exception(self, span: "Any | None", exc: "BaseException") -> "None":
        del span, exc

    def set_status_error(self, span: "Any | None", description: "str") -> "None":
        del span, description

    def set_attribute(self, span: "Any | None", key: "str", value: "object") -> "None":
        del span, key, value

    def inject_trace_context(self, metadata: "dict[str, Any]") -> "None":
        del metadata

    def extract_trace_context(self, metadata: "Mapping[str, Any]") -> "None":
        del metadata

    def record_counter(self, name: "str", value: "int" = 1, *, attributes: "Mapping[str, str]") -> "None":
        self.counters.append((name, value, dict(attributes)))

    def record_gauge_delta(self, name: "str", delta: "int" = 1, *, attributes: "Mapping[str, str]") -> "None":
        del name, delta, attributes

    def record_duration(self, name: "str", seconds: "float", *, attributes: "Mapping[str, str]") -> "None":
        self.durations.append((name, seconds, dict(attributes)))

    def record_histogram(self, name: "str", value: "float", *, unit: "str", attributes: "Mapping[str, str]") -> "None":
        self.histograms.append((name, value, unit, dict(attributes)))


class _BatchSink:
    def __init__(self, *, fail: "bool" = False) -> "None":
        self.fail = fail
        self.batches: "list[tuple[tuple[QueueEvent, tuple[str, ...]], ...]]" = []

    async def publish(self, event: "QueueEvent", *, channels: "Sequence[str]") -> "None":
        self.batches.append(((event, tuple(channels)),))

    async def publish_many(self, batch: "Sequence[tuple[QueueEvent, Sequence[str]]]") -> "None":
        if self.fail:
            msg = "sink unavailable"
            raise RuntimeError(msg)
        self.batches.append(tuple((event, tuple(channels)) for event, channels in batch))


class _FailingWaitBackend(InMemoryQueueBackend):
    __slots__ = ("wait_calls",)

    def __init__(self, config: "QueueConfig") -> "None":
        super().__init__(config)
        self.wait_calls = 0

    async def wait_for_wakeups(self, timeout: "float | None" = None) -> "bool":
        self.wait_calls += 1
        if self.wait_calls == 1:
            msg = "listener disconnected"
            raise RuntimeError(msg)
        await asyncio.sleep(timeout or 0)
        return False


def _config() -> "QueueConfig":
    return QueueConfig(worker=WorkerConfig(placement="external"), queue_backend="memory", execution_backend="local")


async def test_batch_enqueue_records_size_and_coalesced_wakeup() -> "None":
    config = _config()
    runtime = _RecordingRuntime()
    backend = InMemoryQueueBackend(config)
    async with QueueService(config, queue_backend=backend, observability_runtime=runtime):
        await backend.enqueue_many([TaskRequest(f"tasks.batch.{index}") for index in range(3)])

    assert (
        "litestar_queues.enqueue.batch.size",
        3,
        "records",
        {"queue.backend": "memory", "queue.operation": "enqueue_many"},
    ) in runtime.histograms
    assert (
        "litestar_queues.wakeup.emitted",
        1,
        {"queue.backend": "memory", "queue.transport": "asyncio-event"},
    ) in runtime.counters
    assert (
        "litestar_queues.wakeup.coalesced",
        2,
        {"queue.backend": "memory", "queue.transport": "asyncio-event"},
    ) in runtime.counters


async def test_claim_batch_records_actual_short_batch_size() -> "None":
    config = _config()
    runtime = _RecordingRuntime()
    backend = InMemoryQueueBackend(config)
    async with QueueService(config, queue_backend=backend, observability_runtime=runtime) as service:
        await backend.enqueue_many([TaskRequest("tasks.claim.one"), TaskRequest("tasks.claim.two")])
        runtime.histograms.clear()

        claimed = await service.claim_tasks(limit=5)

    assert len(claimed) == 2
    assert runtime.histograms == [
        ("litestar_queues.claim.batch.size", 2, "records", {"queue.backend": "memory", "queue.operation": "claim_many"})
    ]


async def test_worker_records_empty_poll_wait_and_wakeup_to_claim() -> "None":
    config = _config()
    runtime = _RecordingRuntime()
    backend = InMemoryQueueBackend(config)
    async with QueueService(config, queue_backend=backend, observability_runtime=runtime) as service:
        worker = Worker(service, WorkerConfig(poll_interval=0.001, poll_backoff_max=None))

        assert await worker.run_once() == 0
        assert await worker._wait_for_work() is False

        await backend.enqueue("tasks.woken")
        assert await worker._wait_for_work() is True
        assert len(await worker._claim_available(limit=1)) == 1

    assert ("litestar_queues.worker.poll.empty", 1, {"queue.backend": "memory"}) in runtime.counters
    assert any(
        sample[0] == "litestar_queues.worker.poll.delay"
        and sample[2:] == ("s", {"queue.backend": "memory", "worker.wait.kind": "native"})
        for sample in runtime.histograms
    )
    assert any(
        sample[0] == "litestar_queues.worker.wait.duration"
        and sample[2] == {"queue.backend": "memory", "worker.wait.kind": "native"}
        for sample in runtime.durations
    )
    assert any(
        sample[0] == "litestar_queues.worker.wakeup_to_claim.duration"
        and sample[2] == {"queue.backend": "memory", "queue.transport": "asyncio-event"}
        for sample in runtime.durations
    )


async def test_listener_failure_and_reconnect_emit_bounded_metrics() -> "None":
    config = _config()
    runtime = _RecordingRuntime()
    backend = _FailingWaitBackend(config)
    async with QueueService(config, queue_backend=backend, observability_runtime=runtime) as service:
        worker = Worker(service, WorkerConfig(poll_interval=0.001, poll_backoff_max=None))

        assert await worker._wait_for_work() is None
        assert await worker._wait_for_work() is False

    assert (
        "litestar_queues.listener.error",
        1,
        {"queue.backend": "memory", "queue.transport": "asyncio-event", "queue.outcome": "read_failed"},
    ) in runtime.counters
    assert (
        "litestar_queues.listener.reconnect",
        1,
        {"queue.backend": "memory", "queue.transport": "asyncio-event"},
    ) in runtime.counters


async def test_event_buffer_records_successful_flush_and_overflow_drop() -> "None":
    runtime = _RecordingRuntime()
    sink = _BatchSink()
    publisher = QueueEventPublisher(
        sink,
        buffer_config=EventBufferConfig(batch_size=10, max_pending=2, overflow="drop_newest"),
        observability_runtime=runtime,
        transport="custom",
    )

    await publisher.publish(QueueEvent(type="one", scope="task", task_id="task-a"))
    await publisher.publish(QueueEvent(type="two", scope="task", task_id="task-b"))
    await publisher.publish(QueueEvent(type="three", scope="task", task_id="task-c"))
    await publisher.flush_buffer()

    assert (
        "litestar_queues.event.dropped",
        1,
        {"queue.transport": "custom", "queue.outcome": "overflow"},
    ) in runtime.counters
    assert (
        "litestar_queues.event.flush.size",
        2,
        "events",
        {"queue.transport": "custom", "queue.outcome": "success"},
    ) in runtime.histograms
    assert any(
        sample[0] == "litestar_queues.event.flush.duration"
        and sample[2] == {"queue.transport": "custom", "queue.outcome": "success"}
        for sample in runtime.durations
    )


async def test_event_buffer_records_failed_flush_once() -> "None":
    runtime = _RecordingRuntime()
    publisher = QueueEventPublisher(
        _BatchSink(fail=True),
        buffer_config=EventBufferConfig(batch_size=10),
        observability_runtime=runtime,
        transport="custom",
    )
    await publisher.publish(QueueEvent(type="one", scope="task", task_id="task-a"))

    await publisher.flush_buffer()

    assert runtime.histograms == [
        ("litestar_queues.event.flush.size", 1, "events", {"queue.transport": "custom", "queue.outcome": "failed"})
    ]
    assert len([sample for sample in runtime.durations if sample[0] == "litestar_queues.event.flush.duration"]) == 1

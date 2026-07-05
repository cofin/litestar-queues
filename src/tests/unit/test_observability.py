import asyncio
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import pytest

from litestar_queues import QueueConfig, QueueService, Worker, task

if TYPE_CHECKING:
    from collections.abc import Mapping

    from litestar_queues.execution.cloudrun._typing import CloudRunExecutionLike, CloudRunOperation
    from litestar_queues.observability import QueueObservabilityConfig

pytestmark = pytest.mark.anyio


async def test_queue_config_accepts_observability_enable_flags() -> "None":
    """QueueConfig should expose the common package-level observability switches directly."""
    from litestar_queues.observability import QueueObservabilityConfig

    config = QueueConfig(enable_otel=True, enable_prometheus=True)

    observability_config = config.observability_config

    assert observability_config is not None
    assert observability_config.enable_otel is True
    assert observability_config.enable_prometheus is True

    advanced_config = QueueObservabilityConfig(metric_prefix="custom")
    assert QueueConfig(enable_otel=True, observability=advanced_config).observability_config is advanced_config


async def test_enqueue_uses_observability_runtime_for_producer_span_and_context() -> "None":
    """Enqueue should publish bounded producer telemetry and inject trace context into metadata."""
    runtime = FakeObservabilityRuntime()

    @task("observability.enqueue", queue="critical", execution_profile="heavy")
    async def observed_enqueue() -> "str":
        return "ok"

    async with QueueService(QueueConfig(execution_backend="local"), observability_runtime=runtime) as service:
        result = await service.enqueue(observed_enqueue, metadata={"source": "test"})

    assert runtime.started_spans[0].name == "litestar_queues.publish"
    assert runtime.started_spans[0].kind == "producer"
    expected_attributes = {
        "messaging.system": "litestar_queues",
        "messaging.operation.name": "publish",
        "messaging.destination.name": "critical",
        "queue.task.name": "observability.enqueue",
        "queue.execution.backend": "local",
        "queue.execution.profile": "heavy",
    }
    for key, value in expected_attributes.items():
        assert runtime.started_spans[0].attributes[key] == value
    assert runtime.started_spans[0].ended is True
    assert runtime.started_spans[0].attributes["messaging.message.id"] == str(result.id)
    assert result.record is not None
    assert result.record.metadata["_otel_context"] == {"traceparent": "00-test"}
    assert runtime.counters == [
        (
            "litestar_queues.enqueue.count",
            1,
            {
                "messaging.destination.name": "critical",
                "queue.task.name": "observability.enqueue",
                "queue.execution.backend": "local",
                "queue.execution.profile": "heavy",
            },
        )
    ]
    assert runtime.durations[0][0] == "litestar_queues.enqueue.duration"


async def test_execute_record_uses_observability_runtime_for_consumer_span() -> "None":
    """Task execution should extract producer context and finish a consumer span with status labels."""
    runtime = FakeObservabilityRuntime()

    @task("observability.execute")
    async def observed_execute() -> "str":
        return "ok"

    async with QueueService(QueueConfig(execution_backend="local"), observability_runtime=runtime) as service:
        result = await service.enqueue(observed_execute)
        assert result.record is not None
        claimed = await service.get_queue_backend().claim_task(result.id)
        assert claimed is not None
        completed = await service.execute_record(claimed, worker_id="worker-1")

    process_span = runtime.started_spans[-1]
    assert process_span.name == "litestar_queues.process"
    assert process_span.kind == "consumer"
    assert process_span.parent == {"extracted": {"traceparent": "00-test"}}
    assert process_span.ended is True
    assert process_span.attributes["messaging.message.id"] == str(completed.id)
    assert process_span.attributes["queue.task.status"] == "completed"
    assert runtime.counters[-1] == (
        "litestar_queues.task.execution.count",
        1,
        {
            "messaging.destination.name": "default",
            "queue.task.name": "observability.execute",
            "queue.task.status": "completed",
            "queue.execution.backend": "local",
            "queue.execution.profile": "",
        },
    )
    assert runtime.durations[-1][0] == "litestar_queues.task.execution.duration"


async def test_plugin_startup_resolves_runtime_with_litestar_app(monkeypatch: "pytest.MonkeyPatch") -> "None":
    """Plugin startup should pass the actual Litestar app to runtime creation."""
    from litestar import Litestar

    from litestar_queues import QueuePlugin

    runtime = FakeObservabilityRuntime()
    seen_apps: "list[Litestar | None]" = []

    def create_runtime(
        config: "QueueObservabilityConfig | None", *, app: "Litestar | None" = None
    ) -> "FakeObservabilityRuntime":
        assert config is not None
        seen_apps.append(app)
        return runtime

    monkeypatch.setattr("litestar_queues.observability.create_observability_runtime", create_runtime)
    plugin = QueuePlugin(QueueConfig(enable_otel=None, in_app_worker=False))
    app = Litestar(plugins=[plugin])

    await plugin._on_startup(app)
    try:
        service = app.state[plugin.config.queue_service_state_key]
        assert isinstance(service, QueueService)
        assert service.observability_runtime is runtime
        assert seen_apps == [app]
    finally:
        await plugin._on_shutdown(app)


async def test_worker_records_claim_and_loop_error_metrics() -> "None":
    """Worker metrics should use bounded attributes and no task ids."""
    runtime = FakeObservabilityRuntime()
    recovered = asyncio.Event()

    @task("observability.worker")
    async def observed_worker() -> "str":
        return "ok"

    async with QueueService(QueueConfig(execution_backend="local"), observability_runtime=runtime) as service:
        result = await service.enqueue(observed_worker)
        worker = Worker(service)
        assert await worker.run_once() == 1
        await result.wait(timeout=1, poll_interval=0.01)

        transient = _ObservabilityTransientWorker(service, recovered=recovered, poll_interval=0.01)
        await transient.start()

    assert (
        "litestar_queues.worker.claim.count",
        1,
        {"queue.execution.backend": "local", "messaging.destination.name": "default"},
    ) in runtime.counters
    assert (
        "litestar_queues.worker.loop.error.count",
        1,
        {"queue.execution.backend": "local", "worker.error.type": "RuntimeError"},
    ) in runtime.counters
    assert recovered.is_set()


async def test_cloudrun_dispatch_records_span_and_metrics() -> "None":
    """Cloud Run dispatch should emit package-level dispatch telemetry."""
    from litestar_queues.execution.cloudrun import CloudRunExecutionBackend, CloudRunExecutionConfig

    runtime = FakeObservabilityRuntime()

    @task("observability.cloudrun", execution_backend="cloudrun")
    async def observed_cloudrun() -> "str":
        return "ok"

    backend = CloudRunExecutionBackend(
        execution_config=CloudRunExecutionConfig(project_id="project", region="us-central1", job_name="worker"),
        jobs_client=_FakeCloudRunJobsClient(),
    )
    async with QueueService(
        QueueConfig(execution_backend="cloudrun"), execution_backend=backend, observability_runtime=runtime
    ) as service:
        result = await service.enqueue(observed_cloudrun)
        assert result.record is not None
        execution_ref = await backend.dispatch(service, result.record)

    assert execution_ref == "executions/1"
    dispatch_span = runtime.started_spans[-1]
    assert dispatch_span.name == "litestar_queues.dispatch"
    assert dispatch_span.kind == "producer"
    assert dispatch_span.attributes["queue.execution.backend"] == "cloudrun"
    assert (
        "litestar_queues.execution.dispatch.count",
        1,
        {
            "messaging.destination.name": "default",
            "queue.task.name": "observability.cloudrun",
            "queue.execution.backend": "cloudrun",
            "queue.execution.profile": "",
            "queue.execution.status": "dispatched",
        },
    ) in runtime.counters


@dataclass(slots=True)
class FakeSpan:
    name: str
    kind: str
    attributes: dict[str, object]
    parent: object | None = None
    ended: bool = False
    exceptions: list[BaseException] = field(default_factory=list)

    def set_attribute(self, key: str, value: object) -> "None":
        self.attributes[key] = value

    def record_exception(self, exc: "BaseException") -> "None":
        self.exceptions.append(exc)

    def end(self) -> "None":
        self.ended = True


class FakeObservabilityRuntime:
    __slots__ = ("counters", "durations", "enabled", "started_spans")

    def __init__(self) -> "None":
        self.enabled = True
        self.started_spans: "list[FakeSpan]" = []
        self.counters: "list[tuple[str, int, Mapping[str, str]]]" = []
        self.durations: "list[tuple[str, float, Mapping[str, str]]]" = []

    def start_span(
        self, name: "str", *, kind: "str", attributes: "Mapping[str, object]", parent: "object | None" = None
    ) -> "FakeSpan":
        span = FakeSpan(name=name, kind=kind, attributes=dict(attributes), parent=parent)
        self.started_spans.append(span)
        return span

    def set_attribute(self, span: "FakeSpan | None", key: "str", value: "object") -> "None":
        if span is not None:
            span.set_attribute(key, value)

    def record_exception(self, span: "FakeSpan | None", exc: "BaseException") -> "None":
        if span is not None:
            span.record_exception(exc)

    def end_span(self, span: "FakeSpan | None") -> "None":
        if span is not None:
            span.end()

    def inject_trace_context(self, metadata: "dict[str, Any]") -> "None":
        metadata["_otel_context"] = {"traceparent": "00-test"}

    def extract_trace_context(self, metadata: "Mapping[str, Any]") -> "object | None":
        return {"extracted": metadata["_otel_context"]}

    def record_counter(self, name: "str", value: "int" = 1, *, attributes: "Mapping[str, str]") -> "None":
        self.counters.append((name, value, dict(attributes)))

    def record_duration(self, name: "str", seconds: "float", *, attributes: "Mapping[str, str]") -> "None":
        self.durations.append((name, seconds, dict(attributes)))


class _ObservabilityTransientWorker(Worker):
    __slots__ = ("recovered", "run_once_calls")

    def __init__(self, service: "QueueService", *, recovered: "asyncio.Event", poll_interval: "float") -> "None":
        super().__init__(service, poll_interval=poll_interval)
        self.recovered = recovered
        self.run_once_calls = 0

    async def run_once(self) -> "int":
        self.run_once_calls += 1
        if self.run_once_calls == 1:
            msg = "transient worker failure"
            raise RuntimeError(msg)
        self.recovered.set()
        await self.stop()
        return 0


class _FakeCloudRunJobsClient:
    __slots__ = ()

    async def run_job(self, *, request: "dict[str, Any]") -> "CloudRunOperation":
        return _FakeCloudRunOperation()


class _FakeCloudRunOperation:
    __slots__ = ("metadata",)

    def __init__(self) -> "None":
        self.metadata = _FakeCloudRunMetadata()

    async def result(self) -> "CloudRunExecutionLike":
        return _FakeCloudRunExecution()


class _FakeCloudRunMetadata:
    __slots__ = ("name",)

    def __init__(self) -> "None":
        self.name = "executions/1"


class _FakeCloudRunExecution:
    __slots__ = ("cancelled_count", "conditions", "failed_count", "name", "succeeded_count")

    def __init__(self) -> "None":
        self.name = "executions/1"
        self.succeeded_count = 1
        self.failed_count = 0
        self.cancelled_count = 0
        self.conditions: "list[Any] | None" = []

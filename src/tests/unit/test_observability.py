import asyncio
from dataclasses import dataclass, field, fields
from typing import TYPE_CHECKING, Any

import pytest

from litestar_queues import QueueConfig, QueueService, Worker, WorkerConfig, task

if TYPE_CHECKING:
    from collections.abc import Mapping

    from litestar_queues.execution.cloudrun.typing import CloudRunExecutionLike, CloudRunOperation
    from litestar_queues.observability import ObservabilityConfig

pytestmark = pytest.mark.anyio


async def test_queue_config_uses_single_observability_field() -> "None":
    """QueueConfig should keep package-level observability enablement in one field."""
    from litestar_queues.observability import ObservabilityConfig

    observability = ObservabilityConfig(enable_otel=True, enable_prometheus=True)
    config = QueueConfig(worker=WorkerConfig(placement="external"), queue_backend="memory", observability=observability)
    field_names = {config_field.name for config_field in fields(QueueConfig)}

    assert config.observability is observability
    assert observability.enable_otel is True
    assert observability.enable_prometheus is True
    assert "observability_config" not in field_names
    assert "enable_otel" not in field_names
    assert "enable_prometheus" not in field_names


async def test_enqueue_uses_observability_runtime_for_producer_span_and_context() -> "None":
    """Enqueue should publish bounded producer telemetry and inject trace context into metadata."""
    runtime = FakeObservabilityRuntime()

    @task("observability.enqueue", queue="critical", execution_profile="heavy")
    async def observed_enqueue() -> "str":
        return "ok"

    async with QueueService(
        QueueConfig(worker=WorkerConfig(placement="external"), queue_backend="memory", execution_backend="local"),
        observability_runtime=runtime,
    ) as service:
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
            "litestar_queues.enqueue",
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

    async with QueueService(
        QueueConfig(worker=WorkerConfig(placement="external"), queue_backend="memory", execution_backend="local"),
        observability_runtime=runtime,
    ) as service:
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
        "litestar_queues.task.execution",
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
    from litestar_queues.observability import ObservabilityConfig

    runtime = FakeObservabilityRuntime()
    seen_apps: "list[Litestar | None]" = []

    def create_runtime(
        config: "ObservabilityConfig | None",
        *,
        app: "Litestar | None" = None,
        namespace: "object | None" = None,
    ) -> "FakeObservabilityRuntime":
        assert config is not None
        assert namespace is not None
        seen_apps.append(app)
        return runtime

    monkeypatch.setattr("litestar_queues.observability.create_observability_runtime", create_runtime)
    plugin = QueuePlugin(
        QueueConfig(
            queue_backend="memory",
            observability=ObservabilityConfig(enable_otel=None),
            worker=WorkerConfig(placement="external"),
        )
    )
    app = Litestar(plugins=[plugin])

    async with plugin._lifespan(app):
        service = app.state["queue_service"]
        assert isinstance(service, QueueService)
        assert service.observability_runtime is runtime
        assert seen_apps == [app]


async def test_worker_records_claim_and_loop_error_metrics() -> "None":
    """Worker metrics should use bounded attributes and no task ids."""
    runtime = FakeObservabilityRuntime()
    recovered = asyncio.Event()

    @task("observability.worker")
    async def observed_worker() -> "str":
        return "ok"

    async with QueueService(
        QueueConfig(worker=WorkerConfig(placement="external"), queue_backend="memory", execution_backend="local"),
        observability_runtime=runtime,
    ) as service:
        result = await service.enqueue(observed_worker)
        worker = Worker(service)
        assert await worker.run_once() == 1
        await result.wait(timeout=1, poll_interval=0.01)

        transient = _ObservabilityTransientWorker(service, recovered=recovered, poll_interval=0.01)
        await transient.start()

    assert (
        "litestar_queues.worker.claim",
        1,
        {"queue.execution.backend": "local", "messaging.destination.name": "default"},
    ) in runtime.counters
    assert (
        "litestar_queues.worker.loop.error",
        1,
        {"queue.execution.backend": "local", "worker.error.type": "RuntimeError"},
    ) in runtime.counters
    assert recovered.is_set()


async def test_fake_runtime_records_gauge_delta() -> "None":
    """Fake runtime should keep gauge deltas for local assertions."""
    runtime = FakeObservabilityRuntime()

    runtime.record_gauge_delta("litestar_queues.worker.active", 2, attributes={"messaging.destination.name": "default"})
    runtime.record_gauge_delta(
        "litestar_queues.worker.active", -1, attributes={"messaging.destination.name": "default"}
    )

    assert runtime.gauges == [
        ("litestar_queues.worker.active", 2, {"messaging.destination.name": "default"}),
        ("litestar_queues.worker.active", -1, {"messaging.destination.name": "default"}),
    ]


async def test_runtime_records_gauge_delta_with_otel_up_down_counter(monkeypatch: "pytest.MonkeyPatch") -> "None":
    """OTel gauge deltas should use an up/down counter and cache it."""
    from litestar_queues import observability as observability_module
    from litestar_queues.observability import ObservabilityConfig, QueueObservabilityRuntime
    from litestar_queues.typing import otel_metrics

    meter = _FakeOtelMeter()

    def get_meter(*_args: "Any", **_kwargs: "Any") -> "_FakeOtelMeter":
        return meter

    monkeypatch.setattr(observability_module, "OPENTELEMETRY_INSTALLED", True)
    monkeypatch.setattr(otel_metrics, "get_meter", get_meter)

    runtime = QueueObservabilityRuntime(ObservabilityConfig(enable_otel=True))
    runtime.record_gauge_delta(
        "litestar_queues.worker.active", -2, attributes={"messaging.destination.name": "default"}
    )
    runtime.record_gauge_delta("litestar_queues.worker.active", 3, attributes={"messaging.destination.name": "default"})

    assert meter.created_up_down_counters == ["litestar_queues.worker.active"]
    assert meter.up_down_counter.samples == [
        (-2, {"messaging.destination.name": "default"}),
        (3, {"messaging.destination.name": "default"}),
    ]


async def test_runtime_records_gauge_delta_with_prometheus_gauge() -> "None":
    """Prometheus gauge deltas should increment and decrement the same labeled gauge."""
    prometheus_client = pytest.importorskip("prometheus_client")

    from litestar_queues.observability import ObservabilityConfig, QueueObservabilityRuntime

    registry = prometheus_client.CollectorRegistry()
    runtime = QueueObservabilityRuntime(ObservabilityConfig(enable_prometheus=True, prometheus_registry=registry))

    runtime.record_gauge_delta("litestar_queues.worker.active", 2, attributes={"scope": "worker"})
    runtime.record_gauge_delta("litestar_queues.worker.active", -1, attributes={"scope": "worker"})

    assert registry.get_sample_value("litestar_queues_worker_active", labels={"scope": "worker"}) == 1.0


async def test_runtime_rewrites_package_metric_names_from_queue_namespace() -> "None":
    prometheus_client = pytest.importorskip("prometheus_client")

    from litestar_queues.observability import ObservabilityConfig, QueueObservabilityRuntime

    registry = prometheus_client.CollectorRegistry()
    runtime = QueueObservabilityRuntime(
        ObservabilityConfig(enable_prometheus=True, prometheus_registry=registry),
        namespace="dma",
    )

    runtime.record_counter("litestar_queues.wakeup.emitted", attributes={"queue.backend": "memory"})

    assert registry.get_sample_value("dma_wakeup_emitted_total", labels={"queue.backend": "memory"}) == 1.0


async def test_runtime_records_and_caches_value_histograms_with_explicit_units(
    monkeypatch: "pytest.MonkeyPatch",
) -> "None":
    """Transport batch sizes need value histograms rather than counters or durations."""
    from litestar_queues import observability as observability_module
    from litestar_queues.observability import ObservabilityConfig, QueueObservabilityRuntime
    from litestar_queues.typing import otel_metrics

    meter = _FakeOtelMeter()

    def get_meter(*_args: "Any", **_kwargs: "Any") -> "_FakeOtelMeter":
        return meter

    monkeypatch.setattr(observability_module, "OPENTELEMETRY_INSTALLED", True)
    monkeypatch.setattr(otel_metrics, "get_meter", get_meter)

    runtime = QueueObservabilityRuntime(ObservabilityConfig(enable_otel=True))
    attributes = {"queue.backend": "memory", "queue.operation": "enqueue_many"}
    runtime.record_histogram(
        "litestar_queues.enqueue.batch.size", 5, unit="records", attributes=attributes
    )
    runtime.record_histogram(
        "litestar_queues.enqueue.batch.size", 2, unit="records", attributes=attributes
    )

    assert meter.created_histograms == [("litestar_queues.enqueue.batch.size", "records")]
    assert meter.histogram.samples == [
        (5, attributes),
        (2, attributes),
    ]


async def test_value_histogram_uses_unit_specific_prometheus_name_and_buckets() -> "None":
    prometheus_client = pytest.importorskip("prometheus_client")

    from litestar_queues.observability import ObservabilityConfig, QueueObservabilityRuntime

    registry = prometheus_client.CollectorRegistry()
    runtime = QueueObservabilityRuntime(
        ObservabilityConfig(enable_prometheus=True, prometheus_registry=registry)
    )
    attributes = {"queue.backend": "memory", "queue.operation": "enqueue_many"}

    runtime.record_histogram(
        "litestar_queues.enqueue.batch.size", 25, unit="records", attributes=attributes
    )

    assert (
        registry.get_sample_value(
            "litestar_queues_enqueue_batch_size_records_bucket",
            labels={**attributes, "le": "25.0"},
        )
        == 1.0
    )
    assert (
        registry.get_sample_value(
            "litestar_queues_enqueue_batch_size_records_bucket",
            labels={**attributes, "le": "10.0"},
        )
        == 0.0
    )


def test_transport_metric_contract_locks_kind_unit_and_exact_attributes() -> "None":
    from litestar_queues.observability import _TRANSPORT_METRIC_SPECS

    expected = {
        "litestar_queues.enqueue.batch.size": (
            "histogram",
            "records",
            frozenset({"queue.backend", "queue.operation"}),
        ),
        "litestar_queues.wakeup.emitted": (
            "counter",
            "hints",
            frozenset({"queue.backend", "queue.transport"}),
        ),
        "litestar_queues.wakeup.coalesced": (
            "counter",
            "hints",
            frozenset({"queue.backend", "queue.transport"}),
        ),
        "litestar_queues.worker.poll.empty": ("counter", "polls", frozenset({"queue.backend"})),
        "litestar_queues.worker.poll.delay": (
            "histogram",
            "s",
            frozenset({"queue.backend", "worker.wait.kind"}),
        ),
        "litestar_queues.worker.wait.duration": (
            "duration",
            "s",
            frozenset({"queue.backend", "worker.wait.kind"}),
        ),
        "litestar_queues.worker.wakeup_to_claim.duration": (
            "duration",
            "s",
            frozenset({"queue.backend", "queue.transport"}),
        ),
        "litestar_queues.listener.reconnect": (
            "counter",
            "reconnects",
            frozenset({"queue.backend", "queue.transport"}),
        ),
        "litestar_queues.listener.error": (
            "counter",
            "errors",
            frozenset({"queue.backend", "queue.transport", "queue.outcome"}),
        ),
        "litestar_queues.claim.batch.size": (
            "histogram",
            "records",
            frozenset({"queue.backend", "queue.operation"}),
        ),
        "litestar_queues.event.flush.size": (
            "histogram",
            "events",
            frozenset({"queue.transport", "queue.outcome"}),
        ),
        "litestar_queues.event.flush.duration": (
            "duration",
            "s",
            frozenset({"queue.transport", "queue.outcome"}),
        ),
        "litestar_queues.event.dropped": (
            "counter",
            "events",
            frozenset({"queue.transport", "queue.outcome"}),
        ),
    }

    assert {
        name: (spec.kind, spec.unit, spec.attributes)
        for name, spec in _TRANSPORT_METRIC_SPECS.items()
    } == expected


@pytest.mark.parametrize(
    "extra_attribute",
    ["queue.task.id", "queue.channel", "queue.payload", "db.statement", "url.full", "error.message"],
)
def test_transport_metric_contract_rejects_unbounded_attributes(extra_attribute: "str") -> "None":
    from litestar_queues.observability import _validate_transport_metric

    attributes = {
        "queue.backend": "memory",
        "queue.operation": "enqueue_many",
        extra_attribute: "unbounded-value",
    }

    with pytest.raises(ValueError, match="attributes"):
        _validate_transport_metric(
            "litestar_queues.enqueue.batch.size",
            kind="histogram",
            unit="records",
            attributes=attributes,
        )


def test_transport_metric_contract_rejects_missing_attributes_wrong_kind_and_wrong_unit() -> "None":
    from litestar_queues.observability import _validate_transport_metric

    with pytest.raises(ValueError, match="attributes"):
        _validate_transport_metric(
            "litestar_queues.enqueue.batch.size",
            kind="histogram",
            unit="records",
            attributes={"queue.backend": "memory"},
        )
    with pytest.raises(ValueError, match="kind"):
        _validate_transport_metric(
            "litestar_queues.enqueue.batch.size",
            kind="counter",
            unit="records",
            attributes={"queue.backend": "memory", "queue.operation": "enqueue_many"},
        )
    with pytest.raises(ValueError, match="unit"):
        _validate_transport_metric(
            "litestar_queues.enqueue.batch.size",
            kind="histogram",
            unit="events",
            attributes={"queue.backend": "memory", "queue.operation": "enqueue_many"},
        )


def test_disabled_runtime_skips_histogram_validation_and_instrument_allocation() -> "None":
    from litestar_queues.observability import QueueObservabilityRuntime

    runtime = QueueObservabilityRuntime(None)

    runtime.record_histogram(
        "litestar_queues.enqueue.batch.size",
        1,
        unit="records",
        attributes={"unbounded": "ignored"},
    )

    assert runtime._histograms == {}


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
        QueueConfig(worker=WorkerConfig(placement="external"), queue_backend="memory", execution_backend="cloudrun"),
        execution_backend=backend,
        observability_runtime=runtime,
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
        "litestar_queues.execution.dispatch",
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
    __slots__ = ("counters", "durations", "enabled", "gauges", "started_spans")

    def __init__(self) -> "None":
        self.enabled = True
        self.started_spans: "list[FakeSpan]" = []
        self.counters: "list[tuple[str, int, Mapping[str, str]]]" = []
        self.durations: "list[tuple[str, float, Mapping[str, str]]]" = []
        self.gauges: "list[tuple[str, int, Mapping[str, str]]]" = []

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

    def set_status_error(self, span: "FakeSpan | None", description: "str") -> "None":
        if span is not None:
            span.set_attribute("otel.status_description", description)

    def end_span(self, span: "FakeSpan | None") -> "None":
        if span is not None:
            span.end()

    def inject_trace_context(self, metadata: "dict[str, Any]") -> "None":
        metadata["_otel_context"] = {"traceparent": "00-test"}

    def extract_trace_context(self, metadata: "Mapping[str, Any]") -> "object | None":
        return {"extracted": metadata["_otel_context"]}

    def record_counter(self, name: "str", value: "int" = 1, *, attributes: "Mapping[str, str]") -> "None":
        self.counters.append((name, value, dict(attributes)))

    def record_gauge_delta(self, name: "str", delta: "int" = 1, *, attributes: "Mapping[str, str]") -> "None":
        self.gauges.append((name, delta, dict(attributes)))

    def record_duration(self, name: "str", seconds: "float", *, attributes: "Mapping[str, str]") -> "None":
        self.durations.append((name, seconds, dict(attributes)))


class _FakeOtelMetric:
    __slots__ = ("samples",)

    def __init__(self) -> "None":
        self.samples: "list[tuple[float, dict[str, str]]]" = []

    def add(self, delta: "float", *, attributes: "dict[str, str]") -> "None":
        self.samples.append((delta, dict(attributes)))

    def record(self, value: "float", *, attributes: "dict[str, str]") -> "None":
        self.samples.append((value, dict(attributes)))


class _FakeOtelMeter:
    __slots__ = ("created_histograms", "created_up_down_counters", "histogram", "up_down_counter")

    def __init__(self) -> "None":
        self.created_histograms: "list[tuple[str, str]]" = []
        self.created_up_down_counters: "list[str]" = []
        self.histogram = _FakeOtelMetric()
        self.up_down_counter = _FakeOtelMetric()

    def create_histogram(self, name: "str", *, unit: "str") -> "_FakeOtelMetric":
        self.created_histograms.append((name, unit))
        return self.histogram

    def create_up_down_counter(self, name: "str") -> "_FakeOtelMetric":
        self.created_up_down_counters.append(name)
        return self.up_down_counter


class _ObservabilityTransientWorker(Worker):
    __slots__ = ("recovered", "run_once_calls")

    def __init__(self, service: "QueueService", *, recovered: "asyncio.Event", poll_interval: "float") -> "None":
        super().__init__(service, WorkerConfig(poll_interval=poll_interval))
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


async def test_prometheus_metrics_reach_the_default_registry() -> "None":
    """Metrics must land in the registry Litestar's PrometheusController scrapes.

    Passing ``registry=None`` through to prometheus_client means *do not register*,
    so the collectors existed but nothing was ever exported.
    """
    prometheus_client = pytest.importorskip("prometheus_client")

    from litestar_queues.observability import ObservabilityConfig, QueueObservabilityRuntime

    runtime = QueueObservabilityRuntime(ObservabilityConfig(enable_prometheus=True))
    runtime.record_counter("litestar_queues.default_registry", attributes={"queue.backend": "memory"})

    exported = prometheus_client.generate_latest(prometheus_client.REGISTRY).decode()

    assert "litestar_queues_default_registry_total" in exported
    assert (
        prometheus_client.REGISTRY.get_sample_value(
            "litestar_queues_default_registry_total", labels={"queue.backend": "memory"}
        )
        == 1.0
    )


async def test_prometheus_auto_enables_with_litestar_prometheus_middleware() -> "None":
    """A Litestar app wired for Prometheus should switch queue metrics on by itself."""
    pytest.importorskip("prometheus_client")

    from litestar import Litestar
    from litestar.plugins.prometheus import PrometheusConfig

    from litestar_queues.observability import ObservabilityConfig

    config = ObservabilityConfig()
    wired = Litestar(route_handlers=[], middleware=[PrometheusConfig().middleware])
    bare = Litestar(route_handlers=[])

    assert config.should_enable_prometheus(wired) is True
    assert config.should_enable_prometheus(bare) is False
    assert config.should_enable_prometheus() is False
    assert ObservabilityConfig(enable_prometheus=False).should_enable_prometheus(wired) is False


async def test_runtimes_sharing_a_registry_reuse_collectors() -> "None":
    """A service and a worker in one process must not collide on registration."""
    prometheus_client = pytest.importorskip("prometheus_client")

    from litestar_queues.observability import ObservabilityConfig, QueueObservabilityRuntime

    registry = prometheus_client.CollectorRegistry()
    config = ObservabilityConfig(enable_prometheus=True, prometheus_registry=registry)

    QueueObservabilityRuntime(config).record_counter("litestar_queues.shared", attributes={"scope": "a"})
    QueueObservabilityRuntime(config).record_counter("litestar_queues.shared", attributes={"scope": "a"})

    assert registry.get_sample_value("litestar_queues_shared_total", labels={"scope": "a"}) == 2.0


async def test_duration_buckets_cover_long_running_tasks() -> "None":
    """Task durations past the ten-second client default must not all land in +Inf."""
    prometheus_client = pytest.importorskip("prometheus_client")

    from litestar_queues.observability import ObservabilityConfig, QueueObservabilityRuntime

    registry = prometheus_client.CollectorRegistry()
    runtime = QueueObservabilityRuntime(ObservabilityConfig(enable_prometheus=True, prometheus_registry=registry))

    runtime.record_duration("litestar_queues.slow.duration", 120.0, attributes={"scope": "worker"})

    assert (
        registry.get_sample_value(
            "litestar_queues_slow_duration_seconds_bucket", labels={"scope": "worker", "le": "300.0"}
        )
        == 1.0
    )
    assert (
        registry.get_sample_value(
            "litestar_queues_slow_duration_seconds_bucket", labels={"scope": "worker", "le": "60.0"}
        )
        == 0.0
    )


async def test_publish_span_is_the_injected_parent() -> "None":
    """The consumer span must descend from the publish span, not the ambient one.

    ``start_span`` does not make a span current, so injection used to serialise
    whatever context happened to be active -- the HTTP server span, or nothing.
    """
    pytest.importorskip("opentelemetry.sdk")

    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider

    from litestar_queues.observability import ObservabilityConfig, QueueObservabilityRuntime

    provider = TracerProvider()
    runtime = QueueObservabilityRuntime(ObservabilityConfig(enable_otel=True, tracer_provider=provider))

    publish = runtime.start_span("litestar_queues.publish", kind="producer", attributes={})
    assert publish is not None
    metadata: "dict[str, Any]" = {}
    runtime.inject_trace_context(metadata)
    publish_span: "Any" = publish.span
    publish_context = publish_span.get_span_context()
    runtime.end_span(publish)

    assert metadata, "enqueue with no ambient span must still propagate trace context"

    process = runtime.start_span(
        "litestar_queues.process", kind="consumer", attributes={}, parent=runtime.extract_trace_context(metadata)
    )
    assert process is not None
    process_span: "Any" = process.span
    nested: "Any" = provider.get_tracer("nested").start_span("db.query")

    assert process_span.parent.span_id == publish_context.span_id
    assert process_span.get_span_context().trace_id == publish_context.trace_id
    assert nested.parent.span_id == process_span.get_span_context().span_id

    nested.end()
    runtime.end_span(process)
    assert trace.get_current_span() is trace.INVALID_SPAN


async def test_recorded_exception_marks_the_span_failed() -> "None":
    """Trace backends key error rates off span status, not recorded exceptions."""
    pytest.importorskip("opentelemetry.sdk")

    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.trace import StatusCode

    from litestar_queues.observability import ObservabilityConfig, QueueObservabilityRuntime

    runtime = QueueObservabilityRuntime(ObservabilityConfig(enable_otel=True, tracer_provider=TracerProvider()))
    span = runtime.start_span("litestar_queues.process", kind="consumer", attributes={})

    assert span is not None
    runtime.record_exception(span, RuntimeError("boom"))
    failed_span: "Any" = span.span
    assert failed_span.status.status_code is StatusCode.ERROR

    other = runtime.start_span("litestar_queues.process", kind="consumer", attributes={})
    assert other is not None
    runtime.set_status_error(other, "task failed")
    other_span: "Any" = other.span
    assert other_span.status.status_code is StatusCode.ERROR

    runtime.end_span(other)
    runtime.end_span(span)


async def test_stale_recovery_labels_stay_bounded(monkeypatch: "pytest.MonkeyPatch") -> "None":
    """Recovery counts belong in the sample value, never in a label value.

    Emitting them as label values minted a fresh time series for every distinct
    tally of requeued/failed/skipped/handler-needed tasks.
    """
    from litestar_queues.models import StaleTaskRecoveryResult

    runtime = FakeObservabilityRuntime()

    async def recover(_self: "QueueService", **_kwargs: "Any") -> "StaleTaskRecoveryResult":
        return StaleTaskRecoveryResult(requeued=3, failed=2, skipped=0, handler_needed=1)

    monkeypatch.setattr(QueueService, "recover_stale_tasks", recover)

    async with QueueService(
        QueueConfig(worker=WorkerConfig(placement="external"), queue_backend="memory", execution_backend="local"),
        observability_runtime=runtime,
    ) as service:
        worker = Worker(service, WorkerConfig(stale_after=0.01))
        await worker._maybe_requeue_stale()

    stale_samples = [entry for entry in runtime.counters if entry[0] == "litestar_queues.stale_recovery"]

    assert {(value, attributes["queue.stale.outcome"]) for _name, value, attributes in stale_samples} == {
        (3, "requeued"),
        (2, "failed"),
        (1, "handler_needed"),
    }
    for _name, _value, attributes in stale_samples:
        assert set(attributes) == {"queue.execution.backend", "queue.stale.outcome"}
        assert not any(value.isdigit() for value in attributes.values())


async def test_expiry_counter_uses_a_bounded_outcome_label(monkeypatch: "pytest.MonkeyPatch") -> "None":
    from litestar_queues.models import QueuedTaskRecord

    runtime = FakeObservabilityRuntime()
    expired = [QueuedTaskRecord(task_name=f"tasks.expired.{index}", status="expired") for index in range(3)]

    async def expire(_self: "QueueService", **_kwargs: "Any") -> "list[QueuedTaskRecord]":
        return expired

    monkeypatch.setattr(QueueService, "expire_overdue_tasks", expire)

    async with QueueService(
        QueueConfig(worker=WorkerConfig(placement="external"), queue_backend="memory", execution_backend="local"),
        observability_runtime=runtime,
    ) as service:
        worker = Worker(service, WorkerConfig(expiry_check_interval=0.0))
        await worker._maybe_expire_overdue()

    expiry_samples = [entry for entry in runtime.counters if entry[0] == "litestar_queues.expiry"]

    assert expiry_samples == [
        ("litestar_queues.expiry", 3, {"queue.execution.backend": "local", "queue.expiry.outcome": "expired"})
    ]


async def test_correlation_id_round_trips_through_record_metadata() -> "None":
    """A worker must run the task under the correlation ID of the enqueueing request."""
    pytest.importorskip("sqlspec")

    from sqlspec.utils.correlation import CorrelationContext

    from litestar_queues._correlation import bind_correlation_id, capture_correlation_id, reset_correlation_id

    CorrelationContext.set("request-42")
    metadata: "dict[str, Any]" = {}
    capture_correlation_id(metadata)
    CorrelationContext.set(None)

    assert metadata == {"_correlation_id": "request-42"}

    state = bind_correlation_id(metadata)
    assert CorrelationContext.get() == "request-42"
    reset_correlation_id(state)
    assert CorrelationContext.get() is None

    empty = bind_correlation_id({})
    assert CorrelationContext.get() is None
    reset_correlation_id(empty)


def test_core_imports_stay_free_of_optional_telemetry() -> "None":
    """Importing core queue APIs must not drag in OTel, Prometheus, or SQLSpec."""
    import subprocess
    import sys

    code = (
        "import sys; import litestar_queues, litestar_queues.service, litestar_queues.config; "
        "print([m for m in ('opentelemetry', 'prometheus_client', 'sqlspec') if m in sys.modules])"
    )
    completed = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)

    assert completed.stdout.strip() == "[]"


async def test_unset_execution_profile_is_omitted_from_spans_but_kept_on_metrics() -> "None":
    """Spans omit unknown attributes; metrics cannot, because label names are fixed.

    Prometheus binds label names when the collector is constructed, so the key must
    always be present there. An empty label value is the correct encoding: Prometheus
    treats it as equivalent to the label being absent.
    """
    runtime = FakeObservabilityRuntime()

    @task("observability.no_profile")
    async def no_profile() -> "str":
        return "ok"

    async with QueueService(
        QueueConfig(worker=WorkerConfig(placement="external"), queue_backend="memory", execution_backend="local"),
        observability_runtime=runtime,
    ) as service:
        result = await service.enqueue(no_profile)
        claimed = await service.get_queue_backend().claim_task(result.id)
        assert claimed is not None
        await service.execute_record(claimed)

    for span in runtime.started_spans:
        assert "queue.execution.profile" not in span.attributes
    for _name, _value, attributes in runtime.counters:
        if "queue.execution.profile" in attributes:
            assert attributes["queue.execution.profile"] == ""


async def test_counter_instrument_names_carry_no_count_suffix() -> "None":
    """The instrument type already conveys "count"; semconv discourages the suffix."""
    runtime = FakeObservabilityRuntime()

    @task("observability.naming", execution_profile="heavy")
    async def naming() -> "str":
        return "ok"

    async with QueueService(
        QueueConfig(worker=WorkerConfig(placement="external"), queue_backend="memory", execution_backend="local"),
        observability_runtime=runtime,
    ) as service:
        result = await service.enqueue(naming)
        claimed = await service.get_queue_backend().claim_task(result.id)
        assert claimed is not None
        await service.execute_record(claimed)

    recorded = {name for name, _value, _attributes in runtime.counters}

    assert recorded, "expected counter samples"
    assert not any(name.endswith(".count") for name in recorded)
    assert "litestar_queues.enqueue" in recorded
    assert "litestar_queues.task.execution" in recorded


def test_counter_prometheus_names_are_unchanged_by_the_instrument_rename() -> "None":
    """Dropping the OTel ``.count`` suffix must not move any Prometheus series."""
    pytest.importorskip("prometheus_client")

    from litestar_queues.observability import _counter_name

    assert _counter_name("litestar_queues.enqueue", None) == "litestar_queues_enqueue"
    assert _counter_name("litestar_queues.worker.loop.error", None) == "litestar_queues_worker_loop_error"
    assert _counter_name("litestar_queues.stale_recovery", None) == "litestar_queues_stale_recovery"


async def test_two_label_sets_on_one_metric_name_break_the_collector() -> "None":
    """The hazard every execution backend has to design around.

    A collector is registered once per registry with fixed label names, so a
    second emitter using the same metric name with a different label set does not
    produce a second series -- it raises, and takes the recording call with it.
    """
    prometheus_client = pytest.importorskip("prometheus_client")

    from litestar_queues.observability import ObservabilityConfig, QueueObservabilityRuntime

    registry = prometheus_client.CollectorRegistry()
    runtime = QueueObservabilityRuntime(ObservabilityConfig(enable_prometheus=True, prometheus_registry=registry))

    runtime.record_counter("litestar_queues.collision", attributes={"queue.task.status": "completed"})

    with pytest.raises(ValueError, match="label"):
        runtime.record_counter("litestar_queues.collision", attributes={"queue.other.outcome": "recreated"})


async def test_execution_backends_never_share_a_metric_name_with_different_labels() -> "None":
    """Cloud Run reconciliation and Cloud Tasks repair coexist in one process.

    They are separate operations with separate vocabularies, so they are separate
    metric families. Folding repair into ``execution.reconcile`` would have made
    whichever backend recorded second raise.
    """
    prometheus_client = pytest.importorskip("prometheus_client")

    from litestar_queues.observability import ObservabilityConfig, QueueObservabilityRuntime

    registry = prometheus_client.CollectorRegistry()
    runtime = QueueObservabilityRuntime(ObservabilityConfig(enable_prometheus=True, prometheus_registry=registry))
    shared = {
        "messaging.destination.name": "default",
        "queue.task.name": "probe",
        "queue.execution.backend": "cloudrun",
        "queue.execution.profile": "",
    }

    runtime.record_counter(
        "litestar_queues.execution.dispatch", attributes={**shared, "queue.execution.status": "dispatched"}
    )
    runtime.record_counter(
        "litestar_queues.execution.reconcile", attributes={**shared, "queue.task.status": "completed"}
    )
    runtime.record_counter(
        "litestar_queues.execution.dispatch",
        attributes={**shared, "queue.execution.backend": "cloudtasks", "queue.execution.status": "scheduled"},
    )
    runtime.record_counter(
        "litestar_queues.execution.repair",
        attributes={**shared, "queue.execution.backend": "cloudtasks", "queue.repair.outcome": "recreated"},
    )

    assert (
        registry.get_sample_value(
            "litestar_queues_execution_dispatch_total", labels={**shared, "queue.execution.status": "dispatched"}
        )
        == 1.0
    )
    assert (
        registry.get_sample_value(
            "litestar_queues_execution_repair_total",
            labels={**shared, "queue.execution.backend": "cloudtasks", "queue.repair.outcome": "recreated"},
        )
        == 1.0
    )

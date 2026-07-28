"""What a Cloud Tasks queue is allowed to say about itself.

A managed transport is the one topology where telemetry is not a convenience.
Nothing polls these records, so a delivery that quietly stopped arriving looks
exactly like a queue with no work in it, and the only difference an operator can
see is in the signals.

That makes the label set the contract. Every value on these metrics comes from a
fixed vocabulary, because the alternative -- a task id, a delivery name, an
exception string -- is unbounded by construction and turns one broken queue into
a metrics bill. It is also the leak: Google's own errors quote the target URL and
the calling service account, so the raw text can never reach a signal that leaves
the process. The operator's log keeps the original; the shared telemetry gets the
phase.
"""

from types import SimpleNamespace
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import pytest

from litestar_queues import QueueConfig, QueuePlugin, QueueService, WorkerConfig, task
from litestar_queues.events import EventDeliveryConfig, InMemoryQueueEventSink, QueueEventsConfig
from litestar_queues.observability import ObservabilityConfig
from litestar_queues.task import clear_task_registry
from tests.unit.execution.cloudtasks._fakes import ServiceUnavailable

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Mapping

    from litestar_queues.execution.cloudtasks import CloudTasksExecutionBackend, CloudTasksExecutionConfig
    from litestar_queues.models import QueuedTaskRecord
    from tests.unit.execution.cloudtasks._fakes import CreateCall, FakeCloudTasksClient

pytestmark = pytest.mark.anyio

DISPATCH_METRIC = "litestar_queues.execution.dispatch"
DELIVERY_METRIC = "litestar_queues.execution.delivery"
REPAIR_METRIC = "litestar_queues.execution.repair"

DISPATCH_LABEL = "queue.execution.status"
DELIVERY_LABEL = "queue.delivery.outcome"
REPAIR_LABEL = "queue.repair.outcome"

# Written out here rather than imported from the package on purpose: a test that
# reads the same constant the emitter reads proves only that one name was used
# twice. These literals are the contract a dashboard is built against.
DISPATCH_STATUSES = frozenset({"scheduled", "already_exists", "skipped", "error"})
DELIVERY_OUTCOMES = frozenset({"acknowledged", "duplicate", "retry_scheduled", "transient_error"})
REPAIR_OUTCOMES = frozenset({"present", "recreated", "error"})

SHARED_LABELS = frozenset({
    "messaging.destination.name",
    "queue.task.name",
    "queue.execution.backend",
    "queue.execution.profile",
})
"""The label keys every execution-backend metric family already carries.

They are fixed for a reason unrelated to taste: the Prometheus collector for a
metric name is registered once per registry with these exact label names, so a
second emitter that adds or drops one raises instead of recording.
"""

HTTP_NO_CONTENT = 204
HTTP_UNAVAILABLE = 503

SUCCEEDS = "cloudtasks.observed_succeeds"
FAILS_TERMINALLY = "cloudtasks.observed_fails_terminally"
FAILS_THEN_RETRIES = "cloudtasks.observed_fails_then_retries"

# Everything Google routinely puts in an error message and nothing may echo.
SECRETS = ("https://queue-consumer-abcdef-uc.a.run.app", "queues@example-project.iam.gserviceaccount.com")
POISONED_API_ERROR = (
    "403 The principal queues@example-project.iam.gserviceaccount.com is not permitted "
    "to invoke https://queue-consumer-abcdef-uc.a.run.app/_litestar-queues/cloud-tasks"
)

executions: "list[str]" = []


@task(SUCCEEDS)
async def succeeds() -> "None":
    """A task that completes, so a delivery has a durable success to report."""
    executions.append(SUCCEEDS)


@task(FAILS_TERMINALLY, retries=0)
async def fails_terminally() -> "None":
    """A task with no attempts left, so its failure settles the record.

    Raises:
        RuntimeError: Always.
    """
    executions.append(FAILS_TERMINALLY)
    msg = "no attempts left"
    raise RuntimeError(msg)


@task(FAILS_THEN_RETRIES, retries=3)
async def fails_then_retries() -> "None":
    """A task whose failure returns the record to the queue for another attempt.

    Raises:
        RuntimeError: Always.
    """
    executions.append(FAILS_THEN_RETRIES)
    msg = "attempts remain"
    raise RuntimeError(msg)


class RecordedSpan:
    """One started span and everything written onto it."""

    __slots__ = ("attributes", "ended", "exceptions", "kind", "name", "status_descriptions")

    def __init__(self, name: "str", kind: "str", attributes: "Mapping[str, object]") -> "None":
        self.name = name
        self.kind = kind
        self.attributes = dict(attributes)
        self.exceptions: "list[BaseException]" = []
        self.status_descriptions: "list[str]" = []
        self.ended = False


class RecordingRuntime:
    """An observability runtime that keeps everything instead of exporting it."""

    __slots__ = ("counters", "durations", "enabled", "gauges", "spans")

    def __init__(self) -> "None":
        self.enabled = True
        self.spans: "list[RecordedSpan]" = []
        self.counters: "list[tuple[str, int, dict[str, str]]]" = []
        self.durations: "list[tuple[str, float, dict[str, str]]]" = []
        self.gauges: "list[tuple[str, int, dict[str, str]]]" = []

    def start_span(
        self, name: "str", *, kind: "str", attributes: "Mapping[str, object]", parent: "object | None" = None
    ) -> "RecordedSpan":
        """Record a started span.

        Returns:
            The recorded span.
        """
        del parent
        span = RecordedSpan(name, kind, attributes)
        self.spans.append(span)
        return span

    def end_span(self, span: "RecordedSpan | None") -> "None":
        """Mark a span ended."""
        if span is not None:
            span.ended = True

    def record_exception(self, span: "RecordedSpan | None", exc: "BaseException") -> "None":
        """Attach an exception to a span."""
        if span is not None:
            span.exceptions.append(exc)

    def set_status_error(self, span: "RecordedSpan | None", description: "str") -> "None":
        """Mark a span failed with a description."""
        if span is not None:
            span.status_descriptions.append(description)

    def set_attribute(self, span: "RecordedSpan | None", key: "str", value: "object") -> "None":
        """Set one span attribute."""
        if span is not None:
            span.attributes[key] = value

    def inject_trace_context(self, metadata: "dict[str, Any]") -> "None":
        """Inject a stand-in trace context."""
        metadata["_otel_context"] = {"traceparent": "00-selftest"}

    def extract_trace_context(self, metadata: "Mapping[str, Any]") -> "object | None":
        """Return the injected trace context.

        Returns:
            The carrier the producer wrote, or ``None``.
        """
        return metadata.get("_otel_context")

    def record_counter(self, name: "str", value: "int" = 1, *, attributes: "Mapping[str, str]") -> "None":
        """Record one counter sample."""
        self.counters.append((name, value, dict(attributes)))

    def record_gauge_delta(self, name: "str", delta: "int" = 1, *, attributes: "Mapping[str, str]") -> "None":
        """Record one gauge sample."""
        self.gauges.append((name, delta, dict(attributes)))

    def record_duration(self, name: "str", seconds: "float", *, attributes: "Mapping[str, str]") -> "None":
        """Record one duration sample."""
        self.durations.append((name, seconds, dict(attributes)))

    def record_histogram(self, name: "str", value: "float", *, unit: "str", attributes: "Mapping[str, str]") -> "None":
        """Accept value histograms that are outside this test's assertions."""
        del name, value, unit, attributes

    def samples(self, metric: "str") -> "list[dict[str, str]]":
        """Every attribute set recorded under ``metric``.

        Returns:
            The recorded attribute mappings, in order.
        """
        return [attributes for name, _value, attributes in self.counters if name == metric]

    def outcomes(self, metric: "str", label: "str") -> "list[str]":
        """Every outcome value recorded under ``metric``.

        Returns:
            The recorded label values, in order.
        """
        return [attributes[label] for attributes in self.samples(metric)]


class Harness:
    """A Cloud Tasks backend wired to a recording runtime."""

    __slots__ = ("backend", "client", "events", "runtime", "service")

    def __init__(
        self,
        service: "QueueService",
        backend: "CloudTasksExecutionBackend",
        client: "FakeCloudTasksClient",
        runtime: "RecordingRuntime",
        events: "InMemoryQueueEventSink",
    ) -> "None":
        self.service = service
        self.backend = backend
        self.client = client
        self.runtime = runtime
        self.events = events

    async def enqueue(self, task_name: "str") -> "QueuedTaskRecord":
        """Enqueue one record and read it back.

        Returns:
            The persisted record.
        """
        result = await self.service.enqueue(task_name)
        record = await self.service.get_task(result.id)
        assert record is not None
        return record

    def emitted_text(self) -> "str":
        """Everything the process would export, flattened for substring checks.

        Log records are deliberately excluded: the operator's own log keeps the
        original API error, which is what makes the failure diagnosable at all.

        Returns:
            One string holding every metric name, label, span field, and event.
        """
        parts: "list[str]" = []
        for name, _value, attributes in self.runtime.counters:
            parts.extend((name, *attributes.keys(), *attributes.values()))
        for span in self.runtime.spans:
            parts.extend((span.name, span.kind, *span.status_descriptions))
            parts.extend(f"{key}={value!r}" for key, value in span.attributes.items())
            parts.extend(repr(exc) for exc in span.exceptions)
        for event in self.events.events:
            parts.extend((str(event.message), repr(event.payload)))
        return "\n".join(parts)


@pytest.fixture(autouse=True)
def _register_observed_tasks() -> "None":
    """Put this module's tasks back after the suite-wide registry reset.

    A delivery carries only an id, so the consumer resolves the record's task
    name out of the registry -- the same thing a real consumer process does when
    it imports its task modules at startup.
    """
    from litestar_queues.task import get_task_registry

    clear_task_registry()
    executions.clear()
    registry = get_task_registry()
    for task_obj in (succeeds, fails_terminally, fails_then_retries):
        registry[task_obj.name] = task_obj


@pytest.fixture
async def harness(
    shared_storage: "str", cloud_tasks_config: "Callable[..., CloudTasksExecutionConfig]"
) -> "AsyncIterator[Callable[..., Any]]":
    """Build recording Cloud Tasks harnesses that close with the test.

    Yields:
        A factory taking Cloud Tasks config overrides.
    """
    from litestar_queues.execution.cloudtasks import CloudTasksExecutionBackend
    from tests.unit.execution.cloudtasks._fakes import FakeCloudTasksClient

    opened: "list[QueueService]" = []

    async def build(**config_overrides: "Any") -> "Harness":
        execution_config = cloud_tasks_config(**config_overrides)
        client = FakeCloudTasksClient()
        runtime = RecordingRuntime()
        events = InMemoryQueueEventSink()
        backend = CloudTasksExecutionBackend(execution_config=execution_config, client=client)
        service = QueueService(
            QueueConfig(
                queue_backend=shared_storage,
                execution_backend=execution_config,
                worker=WorkerConfig(placement="external"),
                # Unbuffered: these assertions read what one call published.
                events=QueueEventsConfig(delivery=EventDeliveryConfig(sinks=(events,), buffer=None)),
            ),
            execution_backend=backend,
            observability_runtime=runtime,
        )
        await service.open()
        opened.append(service)
        return Harness(service, backend, client, runtime, events)

    yield build

    for service in opened:
        await service.close()


@pytest.fixture
async def delivery(
    shared_storage: "str",
    cloud_tasks_config: "Callable[..., CloudTasksExecutionConfig]",
    monkeypatch: "pytest.MonkeyPatch",
) -> "AsyncIterator[Callable[..., Any]]":
    """Build running consumer applications whose runtime records everything.

    Yields:
        A factory returning the application, its transport, and its runtime.
    """
    from contextlib import AsyncExitStack

    from litestar import Litestar
    from litestar.testing import AsyncTestClient

    from litestar_queues import observability as observability_module
    from litestar_queues.execution.cloudtasks import backend as cloud_tasks_backend
    from tests.unit.execution.cloudtasks._fakes import FakeCloudTasksClient

    stack = AsyncExitStack()

    async def build(**overrides: "Any") -> "SimpleNamespace":
        tasks = FakeCloudTasksClient()
        runtime = RecordingRuntime()
        monkeypatch.setattr(
            cloud_tasks_backend,
            "import_module",
            lambda _module_path: SimpleNamespace(CloudTasksAsyncClient=lambda: tasks),
        )
        monkeypatch.setattr(observability_module, "create_observability_runtime", lambda _config, **_kwargs: runtime)
        execution = cloud_tasks_config(**overrides)
        plugin = QueuePlugin(
            QueueConfig(
                queue_backend=shared_storage,
                execution_backend=execution,
                worker=WorkerConfig(placement="external"),
                observability=ObservabilityConfig(),
            )
        )
        http = await stack.enter_async_context(AsyncTestClient(app=Litestar(plugins=[plugin])))

        async def post(task_id: "Any") -> "Any":
            return await http.post(execution.route_path, json={"version": 1, "task_id": str(task_id)})

        return SimpleNamespace(http=http, tasks=tasks, runtime=runtime, service=plugin.get_service(), post=post)

    yield build

    await stack.aclose()


# --------------------------------------------------------------------------- dispatch


async def test_a_created_delivery_is_counted_as_scheduled(harness: "Callable[..., Any]") -> "None":
    live = await harness()

    record = await live.enqueue(SUCCEEDS)

    assert live.runtime.outcomes(DISPATCH_METRIC, DISPATCH_LABEL) == ["scheduled"]
    sample = live.runtime.samples(DISPATCH_METRIC)[0]
    assert sample["queue.execution.backend"] == "cloudtasks"
    assert sample["queue.task.name"] == SUCCEEDS
    assert sample["messaging.destination.name"] == record.queue


async def test_a_record_that_is_already_gone_is_counted_as_skipped(harness: "Callable[..., Any]") -> "None":
    """Enqueue and schedule are separate steps, so the record can settle between them."""
    live = await harness()
    record = await live.enqueue(SUCCEEDS)
    await live.service.get_queue_backend().cancel_task(record.id)

    assert await live.backend.schedule(live.service, record) is None
    assert live.runtime.outcomes(DISPATCH_METRIC, DISPATCH_LABEL) == ["scheduled", "skipped"]


async def test_a_delivery_google_already_accepted_is_counted_apart_from_a_new_one(
    harness: "Callable[..., Any]",
) -> "None":
    """An ambiguous create is a different operational story from a clean one."""
    live = await harness()
    record = await live.enqueue(SUCCEEDS)

    async def already_there(call: "CreateCall") -> "None":
        from tests.unit.execution.cloudtasks._fakes import AlreadyExists

        msg = "task already exists"
        raise AlreadyExists(msg)

    live.client.on_create = already_there
    current = await live.service.get_task(record.id)
    await live.backend.schedule(live.service, current)

    assert live.runtime.outcomes(DISPATCH_METRIC, DISPATCH_LABEL) == ["scheduled", "already_exists"]


async def test_a_delivery_that_could_not_be_created_is_counted_as_an_error(harness: "Callable[..., Any]") -> "None":
    from litestar_queues.exceptions import QueueDispatchError

    live = await harness()

    async def refuse(call: "CreateCall") -> "None":
        raise ServiceUnavailable(POISONED_API_ERROR)

    live.client.on_create = refuse

    with pytest.raises(QueueDispatchError):
        await live.service.enqueue(SUCCEEDS)

    assert live.runtime.outcomes(DISPATCH_METRIC, DISPATCH_LABEL) == ["error"]


async def test_creating_a_delivery_opens_a_producer_span(harness: "Callable[..., Any]") -> "None":
    """The create call is a real network hop and belongs in the trace as one."""
    live = await harness()

    await live.enqueue(SUCCEEDS)

    dispatch_spans = [span for span in live.runtime.spans if span.name == "litestar_queues.dispatch"]
    assert len(dispatch_spans) == 1
    assert dispatch_spans[0].kind == "producer"
    assert dispatch_spans[0].attributes["queue.execution.backend"] == "cloudtasks"
    assert dispatch_spans[0].ended


# --------------------------------------------------------------------------- repair


async def test_a_delivery_the_transport_still_holds_is_counted_as_present(harness: "Callable[..., Any]") -> "None":
    live = await harness()
    await live.enqueue(SUCCEEDS)

    await live.backend.repair(live.service, limit=10)

    assert live.runtime.outcomes(REPAIR_METRIC, REPAIR_LABEL) == ["present"]


async def test_a_recreated_delivery_is_counted_as_recreated(harness: "Callable[..., Any]") -> "None":
    live = await harness()
    await live.enqueue(SUCCEEDS)
    live.client.existing.clear()

    await live.backend.repair(live.service, limit=10)

    assert live.runtime.outcomes(REPAIR_METRIC, REPAIR_LABEL) == ["recreated"]


async def test_a_repair_that_cannot_reach_the_transport_is_counted_once_as_an_error(
    harness: "Callable[..., Any]",
) -> "None":
    """One pass, one verdict per candidate: a double count would misreport the rate."""
    live = await harness()
    await live.enqueue(SUCCEEDS)

    async def refuse(name: "str") -> "None":
        raise ServiceUnavailable(POISONED_API_ERROR)

    live.client.on_get = refuse

    await live.backend.repair(live.service, limit=10)

    assert live.runtime.outcomes(REPAIR_METRIC, REPAIR_LABEL) == ["error"]


async def test_repair_never_reports_into_the_reconcile_family(harness: "Callable[..., Any]") -> "None":
    """Cloud Run owns that metric name with a different label set entirely."""
    live = await harness()
    await live.enqueue(SUCCEEDS)
    live.client.existing.clear()

    await live.backend.repair(live.service, limit=10)

    assert live.runtime.samples("litestar_queues.execution.reconcile") == []


# --------------------------------------------------------------------------- delivery


async def test_a_delivery_that_ran_the_task_is_counted_as_acknowledged(delivery: "Callable[..., Any]") -> "None":
    live = await delivery()
    record = await live.service.enqueue(succeeds)

    response = await live.post(record.id)

    assert response.status_code == HTTP_NO_CONTENT
    assert live.runtime.outcomes(DELIVERY_METRIC, DELIVERY_LABEL) == ["acknowledged"]


async def test_a_delivery_for_a_record_someone_else_owns_is_counted_as_a_duplicate(
    delivery: "Callable[..., Any]",
) -> "None":
    """At-least-once is the documented boundary, so its rate has to be visible."""
    live = await delivery()
    record = await live.service.enqueue(succeeds)
    claimed, _expired = await live.service.claim_task(record.id)
    assert claimed is not None

    await live.post(record.id)

    assert live.runtime.outcomes(DELIVERY_METRIC, DELIVERY_LABEL) == ["duplicate"]


async def test_a_failure_with_attempts_left_is_counted_apart_from_a_settled_one(
    delivery: "Callable[..., Any]",
) -> "None":
    """A queue whose deliveries are all retries is burning money, not working."""
    live = await delivery()
    retried = await live.service.enqueue(fails_then_retries)
    settled = await live.service.enqueue(fails_terminally)

    await live.post(retried.id)
    await live.post(settled.id)

    assert live.runtime.outcomes(DELIVERY_METRIC, DELIVERY_LABEL) == ["retry_scheduled", "acknowledged"]


async def test_an_unreachable_queue_is_counted_as_a_transient_error(
    delivery: "Callable[..., Any]", monkeypatch: "pytest.MonkeyPatch"
) -> "None":
    live = await delivery()
    record = await live.service.enqueue(succeeds)

    async def unreachable(task_id: "Any") -> "None":
        msg = "queue storage is unreachable"
        raise ConnectionError(msg)

    monkeypatch.setattr(live.service.get_queue_backend(), "get_task", unreachable)

    response = await live.post(record.id)

    assert response.status_code == HTTP_UNAVAILABLE
    assert live.runtime.outcomes(DELIVERY_METRIC, DELIVERY_LABEL) == ["transient_error"]


async def test_a_delivery_for_a_record_that_no_longer_exists_is_counted_as_acknowledged(
    delivery: "Callable[..., Any]",
) -> "None":
    live = await delivery()

    response = await live.post(uuid4())

    assert response.status_code == HTTP_NO_CONTENT
    assert live.runtime.outcomes(DELIVERY_METRIC, DELIVERY_LABEL) == ["acknowledged"]


async def test_a_label_lookup_that_fails_never_revokes_an_earned_acknowledgement(
    delivery: "Callable[..., Any]", monkeypatch: "pytest.MonkeyPatch"
) -> "None":
    """Telemetry decides what a delivery is called, never whether it is answered."""
    live = await delivery()
    record = await live.service.enqueue(fails_then_retries)
    original = QueueService.get_task
    reads = {"count": 0}

    async def fail_after_the_run(self: "QueueService", task_id: "Any") -> "Any":
        # Read one locates the record for the run; read two only names the
        # outcome, and is the one this test takes away. Patched on the class
        # because the service is slotted -- there is one service here anyway.
        reads["count"] += 1
        if reads["count"] > 1:
            msg = "queue storage went away after the run"
            raise ConnectionError(msg)
        return await original(self, task_id)

    monkeypatch.setattr(QueueService, "get_task", fail_after_the_run)

    response = await live.post(record.id)

    assert response.status_code == HTTP_NO_CONTENT
    assert live.runtime.outcomes(DELIVERY_METRIC, DELIVERY_LABEL) == ["acknowledged"]


# --------------------------------------------------------------------------- boundedness


async def test_every_recorded_outcome_comes_from_its_fixed_vocabulary(
    harness: "Callable[..., Any]", delivery: "Callable[..., Any]"
) -> "None":
    """One unbounded label is all it takes to turn a broken queue into a bill."""
    live = await harness()
    await live.enqueue(SUCCEEDS)
    live.client.existing.clear()
    await live.backend.repair(live.service, limit=10)

    served = await delivery()
    record = await served.service.enqueue(succeeds)
    await served.post(record.id)

    for metric, label, allowed in (
        (DISPATCH_METRIC, DISPATCH_LABEL, DISPATCH_STATUSES),
        (REPAIR_METRIC, REPAIR_LABEL, REPAIR_OUTCOMES),
    ):
        assert live.runtime.outcomes(metric, label)
        assert set(live.runtime.outcomes(metric, label)) <= allowed
    assert set(served.runtime.outcomes(DELIVERY_METRIC, DELIVERY_LABEL)) <= DELIVERY_OUTCOMES


async def test_dispatch_and_repair_carry_exactly_the_shared_label_set(harness: "Callable[..., Any]") -> "None":
    """A metric name is registered once per registry, label names and all."""
    live = await harness()
    await live.enqueue(SUCCEEDS)
    live.client.existing.clear()
    await live.backend.repair(live.service, limit=10)

    for metric, label in ((DISPATCH_METRIC, DISPATCH_LABEL), (REPAIR_METRIC, REPAIR_LABEL)):
        samples = live.runtime.samples(metric)
        assert samples
        for sample in samples:
            assert set(sample) == SHARED_LABELS | {label}


async def test_the_delivery_metric_carries_only_what_the_transport_knows(delivery: "Callable[..., Any]") -> "None":
    """The route holds an id, not a record; anything richer would be a second read."""
    live = await delivery()
    record = await live.service.enqueue(succeeds)

    await live.post(record.id)

    assert live.runtime.samples(DELIVERY_METRIC) == [
        {"queue.execution.backend": "cloudtasks", DELIVERY_LABEL: "acknowledged"}
    ]


async def test_no_task_id_or_delivery_name_ever_reaches_a_metric(harness: "Callable[..., Any]") -> "None":
    """Both are unique per record, which is the definition of an unbounded label."""
    live = await harness()
    record = await live.enqueue(SUCCEEDS)
    live.client.existing.clear()
    await live.backend.repair(live.service, limit=10)

    current = await live.service.get_task(record.id)
    for _name, _value, attributes in live.runtime.counters:
        for value in attributes.values():
            assert record.id.hex not in value
            assert str(record.id) not in value
            assert value != current.execution_ref


# --------------------------------------------------------------------------- sanitization


async def test_a_google_error_never_reaches_a_metric_span_or_event(harness: "Callable[..., Any]") -> "None":
    """Google quotes the target URL and the calling identity in its own messages."""
    live = await harness()

    async def refuse(call: "CreateCall") -> "None":
        raise ServiceUnavailable(POISONED_API_ERROR)

    live.client.on_create = refuse
    with pytest.raises(Exception, match="Cloud Tasks delivery could not be created"):
        await live.service.enqueue(SUCCEEDS)

    emitted = live.emitted_text()
    assert "403 The principal" not in emitted
    for secret in SECRETS:
        assert secret not in emitted


async def test_a_failed_dispatch_span_carries_the_phase_not_the_exception(harness: "Callable[..., Any]") -> "None":
    """Recording the exception object onto the span would serialize its message."""
    live = await harness()

    async def refuse(call: "CreateCall") -> "None":
        raise ServiceUnavailable(POISONED_API_ERROR)

    live.client.on_create = refuse
    with pytest.raises(Exception, match="Cloud Tasks delivery could not be created"):
        await live.service.enqueue(SUCCEEDS)

    dispatch_span = next(span for span in live.runtime.spans if span.name == "litestar_queues.dispatch")
    assert dispatch_span.exceptions == []
    assert dispatch_span.status_descriptions == ["cloudtasks.schedule_failed"]


async def test_task_arguments_never_reach_a_signal(harness: "Callable[..., Any]") -> "None":
    """Arguments are the one thing deliberately kept off the wire; keep them off signals too."""
    live = await harness()

    @task("cloudtasks.observed_with_secrets")
    async def with_secrets(api_key: "str") -> "None":
        return None

    await live.service.enqueue(with_secrets, api_key="sk-do-not-log-me")

    assert "sk-do-not-log-me" not in live.emitted_text()


async def test_the_operator_log_still_carries_the_original_error(
    harness: "Callable[..., Any]", caplog: "pytest.LogCaptureFixture"
) -> "None":
    """Sanitizing shared telemetry is not the same as making the failure undiagnosable."""
    live = await harness()

    async def refuse(call: "CreateCall") -> "None":
        raise ServiceUnavailable(POISONED_API_ERROR)

    live.client.on_create = refuse
    with caplog.at_level("WARNING"), pytest.raises(Exception, match="Cloud Tasks delivery could not be created"):
        await live.service.enqueue(SUCCEEDS)

    assert POISONED_API_ERROR in caplog.text

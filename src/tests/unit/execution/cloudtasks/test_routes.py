"""What the delivery route owes Cloud Tasks, and what it must never owe it.

Cloud Tasks reads one thing off a response: whether it was 2xx. Every other
status is a signal to deliver the same task again on the queue's retry policy.
That inverts the usual HTTP instinct. A 404 for a record that no longer exists,
or a 409 for a delivery that lost its claim, would each be honest and each would
put Google into a redelivery loop over an answer that can never change.

So the route acknowledges every outcome the queue reached durably -- including
the failures -- and reserves a retryable status for the one case where the same
delivery genuinely should arrive again: the queue could not be reached, or the
retry it just scheduled never made it to Google.

The one thing the route never acknowledges is a request it could not
authenticate or parse, because that is a deployment fault an operator has to
see rather than a queue outcome.
"""

from contextlib import AsyncExitStack
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid4

import pytest
from litestar import Litestar
from litestar.exceptions import NotAuthorizedException
from litestar.testing import AsyncTestClient

from litestar_queues import QueueConfig, QueuePlugin, WorkerConfig, task

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Iterator

    from litestar.connection import ASGIConnection
    from litestar.handlers.base import BaseRouteHandler

    from litestar_queues import QueueService
    from litestar_queues.execution.cloudtasks import CloudTasksExecutionConfig
    from litestar_queues.models import QueuedTaskRecord
    from tests.unit.execution.cloudtasks._fakes import CreateCall, FakeCloudTasksClient

pytestmark = pytest.mark.anyio

HTTP_NO_CONTENT = 204
HTTP_BAD_REQUEST = 400
HTTP_UNAUTHORIZED = 401
HTTP_NOT_FOUND = 404
HTTP_UNAVAILABLE = 503

SUCCEEDS = "cloudtasks.route_succeeds"
FAILS_TERMINALLY = "cloudtasks.route_fails_terminally"
FAILS_THEN_RETRIES = "cloudtasks.route_fails_then_retries"
NEVER_REGISTERED = "cloudtasks.route_never_registered"

executions: "list[str]" = []


@task(SUCCEEDS)
async def succeeds() -> "None":
    """A task that completes, so the delivery has a durable success to report."""
    executions.append(SUCCEEDS)


@task(FAILS_TERMINALLY, retries=0)
async def fails_terminally() -> "None":
    """A task with no retries left, so failing it is a terminal outcome.

    Raises:
        RuntimeError: Always.
    """
    executions.append(FAILS_TERMINALLY)
    msg = "this task always fails"
    raise RuntimeError(msg)


@task(FAILS_THEN_RETRIES, retries=3)
async def fails_then_retries() -> "None":
    """A task whose failure returns the record to pending for another attempt.

    Raises:
        RuntimeError: Always.
    """
    executions.append(FAILS_THEN_RETRIES)
    msg = "this task fails but has attempts left"
    raise RuntimeError(msg)


@pytest.fixture(autouse=True)
def _register_route_tasks() -> "None":
    """Undo the suite-wide registry reset for this module's tasks.

    A delivery only carries an id, so the consumer resolves the record's task
    name out of the registry. These tasks are declared once at module scope and
    put back per test, which is also what a real consumer process does when it
    imports its task modules at startup.
    """
    from litestar_queues.task import get_task_registry

    executions.clear()
    registry = get_task_registry()
    for task_obj in (succeeds, fails_terminally, fails_then_retries):
        registry[task_obj.name] = task_obj


def _deny_everything(connection: "ASGIConnection[Any, Any, Any, Any]", handler: "BaseRouteHandler") -> "None":
    """Reject every caller.

    Raises:
        NotAuthorizedException: Always.
    """
    del connection, handler
    raise NotAuthorizedException


class Delivery:
    """One running consumer application and the transport that feeds it."""

    __slots__ = ("http", "path", "plugin", "tasks")

    def __init__(
        self, http: "AsyncTestClient[Litestar]", tasks: "FakeCloudTasksClient", plugin: "QueuePlugin", path: "str"
    ) -> "None":
        self.http = http
        self.tasks = tasks
        self.plugin = plugin
        self.path = path

    @property
    def service(self) -> "QueueService":
        """The queue service the application opened."""
        return self.plugin.get_service()

    async def deliver(self, task_id: "UUID | str", *, version: "int" = 1, **kwargs: "Any") -> "Any":
        """POST one well-formed delivery for a task id.

        Returns:
            The consumer's HTTP response.
        """
        return await self.http.post(self.path, json={"version": version, "task_id": str(task_id)}, **kwargs)

    async def record(self, task_id: "UUID") -> "QueuedTaskRecord | None":
        """Read a record back out of the store.

        Returns:
            The current record, or ``None`` when it is gone.
        """
        return await self.service.get_task(task_id)


@pytest.fixture
def shared_store() -> "Iterator[str]":
    """Register a backend name whose every construction is the same store.

    Cloud Run runs several consumer instances against one database, and each of
    them builds its own backend object. Returning one instance is how a
    single-process test can tell "two applications, one queue" apart from "two
    applications, two queues".

    Yields:
        The registered queue backend name.
    """
    from litestar_queues.backends.factory import _queue_backend_registry
    from litestar_queues.backends.memory import InMemoryQueueBackend

    class _SharedStandIn(InMemoryQueueBackend):
        """The store itself. Unslotted so a test can patch a method onto it."""

    store = _SharedStandIn()

    class _OneStore(_SharedStandIn):
        """Hands back the one store. Never initialized: it is not an instance of itself."""

        def __new__(cls, *args: "Any", **kwargs: "Any") -> "_SharedStandIn":  # type: ignore[misc]
            del args, kwargs
            return store

    name = "selftest-one-store"
    _queue_backend_registry[name] = _OneStore
    yield name
    _queue_backend_registry.pop(name, None)


@pytest.fixture
async def delivery(
    shared_store: "str",
    cloud_tasks_config: "Callable[..., CloudTasksExecutionConfig]",
    monkeypatch: "pytest.MonkeyPatch",
) -> "AsyncIterator[Callable[..., Any]]":
    """Build running consumer applications that shut down with the test.

    The Google client is injected through the backend's own lazy import rather
    than passed in, so these tests exercise the same client resolution a real
    deployment uses.

    Yields:
        A factory taking queue-config and Cloud Tasks-config overrides.
    """
    from litestar_queues.execution.cloudtasks import backend as cloud_tasks_backend
    from tests.unit.execution.cloudtasks._fakes import FakeCloudTasksClient

    stack = AsyncExitStack()

    async def build(
        *,
        execution_backend: "Any" = None,
        app_guards: "tuple[Any, ...]" = (),
        queue_backend: "str | None" = None,
        initialize_schedules: "bool" = True,
        **overrides: "Any",
    ) -> "Delivery":
        tasks = FakeCloudTasksClient()
        monkeypatch.setattr(
            cloud_tasks_backend,
            "import_module",
            lambda _module_path: SimpleNamespace(CloudTasksAsyncClient=lambda: tasks),
        )
        execution = cloud_tasks_config(**overrides) if execution_backend is None else execution_backend
        plugin = QueuePlugin(
            QueueConfig(
                queue_backend=queue_backend or shared_store,
                execution_backend=execution,
                worker=WorkerConfig(placement="external"),
                initialize_schedules=initialize_schedules,
            )
        )
        app = Litestar(plugins=[plugin], guards=list(app_guards))
        http = await stack.enter_async_context(AsyncTestClient(app=app))
        path = getattr(execution, "route_path", "/_litestar-queues/cloud-tasks")
        return Delivery(http, tasks, plugin, path)

    yield build

    await stack.aclose()


# --------------------------------------------------------------------------- registration


async def test_a_cloud_tasks_queue_exposes_a_delivery_route(delivery: "Callable[..., Any]") -> "None":
    live = await delivery()

    record = await live.service.enqueue(succeeds)
    response = await live.deliver(record.id)

    assert response.status_code == HTTP_NO_CONTENT
    assert executions == [SUCCEEDS]


async def test_a_polled_queue_exposes_no_delivery_route(delivery: "Callable[..., Any]") -> "None":
    """Only a queue that is actually delivered to over HTTP opens a door for it."""
    live = await delivery(execution_backend="local", queue_backend="memory")

    response = await live.http.post("/_litestar-queues/cloud-tasks", json={"version": 1, "task_id": str(uuid4())})

    assert response.status_code == HTTP_NOT_FOUND


async def test_the_route_is_mounted_where_the_producer_points(delivery: "Callable[..., Any]") -> "None":
    """A delivery is addressed by the producer, so the two paths cannot drift."""
    live = await delivery(route_path="/internal/deliveries")
    execution_config = live.service.config.execution_backend

    record = await live.service.enqueue(succeeds)
    call: "CreateCall" = live.tasks.create_calls[0]
    posted_to = str(call.http_request["url"])

    assert posted_to == execution_config.target_url
    assert posted_to.endswith("/internal/deliveries")
    assert (await live.deliver(record.id)).status_code == HTTP_NO_CONTENT


async def test_exactly_one_delivery_route_is_registered(delivery: "Callable[..., Any]") -> "None":
    live = await delivery()

    mounted = [route for route in live.http.app.routes if route.path == live.path]

    assert len(mounted) == 1
    assert set(mounted[0].methods) >= {"POST"}


# --------------------------------------------------------------------------- request contract


async def test_the_producer_and_the_route_agree_on_the_protocol_version(delivery: "Callable[..., Any]") -> "None":
    """One constant, read by the side that writes the body and the side that reads it."""
    from litestar.serialization import decode_json

    from litestar_queues.execution.cloudtasks import CLOUD_TASKS_PROTOCOL_VERSION

    live = await delivery()

    record = await live.service.enqueue(succeeds)
    body = decode_json(live.tasks.create_calls[0].body)

    assert body["version"] == CLOUD_TASKS_PROTOCOL_VERSION
    # The bytes the producer built, replayed verbatim into the route.
    response = await live.http.post(live.path, content=live.tasks.create_calls[0].body)
    assert response.status_code == HTTP_NO_CONTENT
    assert body["task_id"] == str(record.id)


async def test_a_body_carrying_an_unknown_field_is_refused(delivery: "Callable[..., Any]") -> "None":
    """The transport carries an id and nothing else; anything more is not ours."""
    live = await delivery()
    record = await live.service.enqueue(succeeds)

    response = await live.http.post(live.path, json={"version": 1, "task_id": str(record.id), "args": ["injected"]})

    assert response.status_code == HTTP_BAD_REQUEST
    assert executions == []


async def test_a_body_with_no_task_id_is_refused(delivery: "Callable[..., Any]") -> "None":
    live = await delivery()

    response = await live.http.post(live.path, json={"version": 1})

    assert response.status_code == HTTP_BAD_REQUEST


async def test_a_task_id_that_is_not_an_identifier_is_refused(delivery: "Callable[..., Any]") -> "None":
    live = await delivery()

    response = await live.http.post(live.path, json={"version": 1, "task_id": "../../etc/passwd"})

    assert response.status_code == HTTP_BAD_REQUEST


async def test_an_unknown_protocol_version_is_refused(delivery: "Callable[..., Any]") -> "None":
    """A body this build cannot read is a deployment fault, not a queue outcome."""
    live = await delivery()
    record = await live.service.enqueue(succeeds)

    response = await live.deliver(record.id, version=2)

    assert response.status_code == HTTP_BAD_REQUEST
    assert executions == []


# --------------------------------------------------------------------------- authentication


async def test_a_route_guard_rejects_a_delivery_before_any_queue_work(delivery: "Callable[..., Any]") -> "None":
    live = await delivery(guards=(_deny_everything,), trust_platform_auth=False)
    record = await live.service.enqueue(succeeds)

    response = await live.deliver(record.id)

    assert response.status_code == HTTP_UNAUTHORIZED
    assert executions == []
    current = await live.record(record.id)
    assert current is not None
    assert current.status == "pending"


async def test_an_application_guard_still_covers_the_delivery_route(delivery: "Callable[..., Any]") -> "None":
    """The route is an ordinary handler, so app-level protection composes normally."""
    live = await delivery(app_guards=(_deny_everything,))
    record = await live.service.enqueue(succeeds)

    response = await live.deliver(record.id)

    assert response.status_code == HTTP_UNAUTHORIZED
    assert executions == []


async def test_delivery_headers_are_never_treated_as_authentication(delivery: "Callable[..., Any]") -> "None":
    """Anyone can send these headers. Only the platform or a guard says who called."""
    live = await delivery(guards=(_deny_everything,), trust_platform_auth=False)
    record = await live.service.enqueue(succeeds)

    response = await live.deliver(
        record.id,
        headers={
            "X-CloudTasks-TaskName": "projects/example-project/locations/us-central1/queues/q/tasks/forged",
            "X-CloudTasks-QueueName": "queue-consumer",
            "X-CloudTasks-TaskRetryCount": "0",
            "User-Agent": "Google-Cloud-Tasks",
        },
    )

    assert response.status_code == HTTP_UNAUTHORIZED
    assert executions == []


# --------------------------------------------------------------------------- acknowledged outcomes


async def test_a_completed_task_is_acknowledged(delivery: "Callable[..., Any]") -> "None":
    live = await delivery()
    record = await live.service.enqueue(succeeds)

    response = await live.deliver(record.id)

    assert response.status_code == HTTP_NO_CONTENT
    current = await live.record(record.id)
    assert current is not None
    assert current.status == "completed"


async def test_a_delivery_for_a_record_that_is_gone_is_acknowledged(delivery: "Callable[..., Any]") -> "None":
    """A delivery whose record vanished can never become valid, so retrying it is waste."""
    live = await delivery()

    response = await live.deliver(uuid4())

    assert response.status_code == HTTP_NO_CONTENT


async def test_a_delivery_for_an_unregistered_task_is_acknowledged_and_failed(delivery: "Callable[..., Any]") -> "None":
    """Redelivering will not teach this process a task name it does not have.

    The record has to reach a terminal state here, because on this queue nothing
    else is watching it: no poller will ever come back to expire it.
    """
    live = await delivery()
    record = await live.service.get_queue_backend().enqueue(NEVER_REGISTERED, max_retries=0)

    response = await live.deliver(record.id)

    assert response.status_code == HTTP_NO_CONTENT
    current = await live.record(record.id)
    assert current is not None
    assert current.status == "failed"
    assert current.is_terminal


async def test_a_task_that_failed_for_the_last_time_is_acknowledged(delivery: "Callable[..., Any]") -> "None":
    live = await delivery()
    record = await live.service.enqueue(fails_terminally)

    response = await live.deliver(record.id)

    assert response.status_code == HTTP_NO_CONTENT
    current = await live.record(record.id)
    assert current is not None
    assert current.status == "failed"


async def test_a_task_that_will_be_retried_is_acknowledged_with_its_next_delivery_made(
    delivery: "Callable[..., Any]",
) -> "None":
    """The retry is a new delivery, not this one arriving again."""
    live = await delivery()
    record = await live.service.enqueue(fails_then_retries)
    deliveries_before = len(live.tasks.create_calls)

    response = await live.deliver(record.id)

    assert response.status_code == HTTP_NO_CONTENT
    current = await live.record(record.id)
    assert current is not None
    assert current.status in {"pending", "scheduled"}
    assert len(live.tasks.create_calls) == deliveries_before + 1


async def test_a_cancelled_record_is_acknowledged(delivery: "Callable[..., Any]") -> "None":
    """The store's decision outlives a delivery Google was already holding."""
    live = await delivery()
    record = await live.service.enqueue(succeeds)
    await live.service.get_queue_backend().cancel_task(record.id)

    response = await live.deliver(record.id)

    assert response.status_code == HTTP_NO_CONTENT
    assert executions == []


async def test_a_delivery_that_loses_the_claim_is_acknowledged(delivery: "Callable[..., Any]") -> "None":
    """Someone else owns this attempt; running it again would double the work."""
    live = await delivery()
    record = await live.service.enqueue(succeeds)
    claimed, _expired = await live.service.claim_task(record.id)
    assert claimed is not None

    response = await live.deliver(record.id)

    assert response.status_code == HTTP_NO_CONTENT
    assert executions == []


async def test_a_second_delivery_after_completion_is_acknowledged(delivery: "Callable[..., Any]") -> "None":
    live = await delivery()
    record = await live.service.enqueue(succeeds)

    first = await live.deliver(record.id)
    second = await live.deliver(record.id)

    assert (first.status_code, second.status_code) == (HTTP_NO_CONTENT, HTTP_NO_CONTENT)
    assert executions == [SUCCEEDS]


# --------------------------------------------------------------------------- redelivery is asked for


async def test_a_queue_that_cannot_be_read_asks_for_redelivery(
    delivery: "Callable[..., Any]", monkeypatch: "pytest.MonkeyPatch"
) -> "None":
    """Storage being unreachable says nothing about the task, so the delivery must survive."""
    live = await delivery()
    record = await live.service.enqueue(succeeds)

    async def unreachable(task_id: "UUID") -> "QueuedTaskRecord | None":
        msg = "queue storage is unreachable"
        raise ConnectionError(msg)

    monkeypatch.setattr(live.service.get_queue_backend(), "get_task", unreachable)

    response = await live.deliver(record.id)

    assert response.status_code == HTTP_UNAVAILABLE


async def test_a_retry_whose_delivery_could_not_be_created_asks_for_redelivery(
    delivery: "Callable[..., Any]",
) -> "None":
    """The record is pending with no delivery; this request coming back is what fixes it."""
    from tests.unit.execution.cloudtasks._fakes import ServiceUnavailable

    live = await delivery()
    record = await live.service.enqueue(fails_then_retries)

    async def refuse(call: "CreateCall") -> "None":
        msg = "the API did not answer"
        raise ServiceUnavailable(msg)

    live.tasks.on_create = refuse

    response = await live.deliver(record.id)

    assert response.status_code == HTTP_UNAVAILABLE


async def test_a_retryable_response_carries_no_diagnostic_text(
    delivery: "Callable[..., Any]", monkeypatch: "pytest.MonkeyPatch"
) -> "None":
    """Cloud Tasks logs response bodies, and a storage error routinely quotes a DSN."""
    live = await delivery()
    record = await live.service.enqueue(succeeds)

    async def leaky(task_id: "UUID") -> "QueuedTaskRecord | None":
        msg = "postgres://queue_admin:hunter2@10.0.0.4:5432/queue is unreachable"
        raise ConnectionError(msg)

    monkeypatch.setattr(live.service.get_queue_backend(), "get_task", leaky)

    response = await live.deliver(record.id)

    assert response.status_code == HTTP_UNAVAILABLE
    assert b"hunter2" not in response.content
    assert b"10.0.0.4" not in response.content
    assert response.content in {b"", b"null"}


# --------------------------------------------------------------------------- lifecycle


async def test_a_consumer_application_starts_no_worker(delivery: "Callable[..., Any]") -> "None":
    """The whole point of this topology is that nothing polls.

    A worker here would poll the queue from inside the web container, which is
    the always-on cost the managed transport exists to remove -- and it would
    race Cloud Tasks for every record.
    """
    from litestar_queues.config import _WORKER_STATE_KEY

    live = await delivery()

    assert _WORKER_STATE_KEY not in live.http.app.state
    assert live.plugin._worker is None
    assert live.plugin._worker_task is None


async def test_recurring_schedules_are_created_without_a_worker(delivery: "Callable[..., Any]") -> "None":
    """Schedules normally belong to the worker, and there is no worker to give them to.

    Somebody still has to write the first occurrence and hand it to the
    transport, and on this topology the web process is the only candidate.
    """
    every_minute = "cloudtasks.route_every_minute"

    @task(every_minute, interval=60)
    async def recurring() -> "None":
        executions.append(every_minute)

    live = await delivery()
    scheduled = await live.service.get_queue_backend().get_task_by_key(f"scheduled:{every_minute}")

    assert scheduled is not None
    assert scheduled.task_name == every_minute
    # Written and delivered: a schedule record nobody handed to Google is a
    # record that never runs.
    assert scheduled.execution_ref is not None
    assert [call.name for call in live.tasks.create_calls] == [scheduled.execution_ref]


async def test_an_application_that_opts_out_writes_no_schedule(delivery: "Callable[..., Any]") -> "None":
    every_minute = "cloudtasks.route_opted_out"

    @task(every_minute, interval=60)
    async def recurring() -> "None":
        executions.append(every_minute)

    live = await delivery(initialize_schedules=False)
    scheduled = await live.service.get_queue_backend().get_task_by_key(f"scheduled:{every_minute}")

    assert scheduled is None
    assert live.tasks.create_calls == []


async def test_two_consumer_instances_share_one_authoritative_occurrence(delivery: "Callable[..., Any]") -> "None":
    """Cloud Run runs several instances, and they all start against one database.

    Each writes the schedule at startup, so without deduplication on the
    schedule key every instance would create its own occurrence and the task
    would run once per instance.
    """
    every_minute = "cloudtasks.route_two_instances"

    @task(every_minute, interval=60)
    async def recurring() -> "None":
        executions.append(every_minute)

    first = await delivery()
    second = await delivery()

    scheduled = await second.service.get_queue_backend().get_task_by_key(f"scheduled:{every_minute}")
    assert scheduled is not None
    # The second instance found the first instance's record and left it alone.
    assert len(first.tasks.create_calls) == 1
    assert second.tasks.create_calls == []
    assert (await first.service.get_task(scheduled.id)) is not None


async def test_the_delivery_client_is_released_when_the_application_stops(
    shared_store: "str",
    cloud_tasks_config: "Callable[..., CloudTasksExecutionConfig]",
    monkeypatch: "pytest.MonkeyPatch",
) -> "None":
    """The producer client is opened once per application and closed with it."""
    from litestar_queues.execution.cloudtasks import backend as cloud_tasks_backend
    from tests.unit.execution.cloudtasks._fakes import FakeCloudTasksClient

    tasks = FakeCloudTasksClient()
    monkeypatch.setattr(
        cloud_tasks_backend, "import_module", lambda _module_path: SimpleNamespace(CloudTasksAsyncClient=lambda: tasks)
    )
    plugin = QueuePlugin(
        QueueConfig(
            queue_backend=shared_store,
            execution_backend=cloud_tasks_config(),
            worker=WorkerConfig(placement="external"),
        )
    )

    async with AsyncTestClient(app=Litestar(plugins=[plugin])) as http:
        await plugin.get_service().enqueue(succeeds)
        assert (
            await http.post("/_litestar-queues/cloud-tasks", json={"version": 1, "task_id": str(uuid4())})
        ).status_code == HTTP_NO_CONTENT
        assert tasks.close_calls == 0

    assert tasks.close_calls == 1

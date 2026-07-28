"""What the Cloud Tasks producer sends, names, and refuses to leak.

Cloud Tasks holds the queue record's delivery for up to thirty days, so whatever
this backend puts in the request is what an attacker, a log sink, or a support
engineer can read for that whole window. The contract is therefore narrow: the
transport carries a version and a task id, nothing else, and the record is
already durable with its delivery name persisted before Google is asked to
create anything.

The client is always injected. ``google-cloud-tasks`` is never imported here --
the request has to be ordinary Python data or these assertions could not exist.
"""

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

import pytest

from litestar_queues import QueueConfig, QueueService, WorkerConfig
from litestar_queues.events import EventDeliveryConfig, InMemoryQueueEventSink, QueueEventsConfig
from litestar_queues.exceptions import QueueConfigurationError, QueueDispatchError
from tests.unit.execution.cloudtasks._fakes import AlreadyExists, FakeCloudTasksClient, ServiceUnavailable

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable

    from litestar_queues.execution.cloudtasks import CloudTasksExecutionBackend, CloudTasksExecutionConfig
    from litestar_queues.models import QueuedTaskRecord

pytestmark = pytest.mark.anyio

TASK_NAME = "cloudtasks.producer_probe"


class Harness:
    """A live service whose Cloud Tasks backend talks to an injected fake."""

    __slots__ = ("backend", "client", "events", "execution_config", "service")

    def __init__(
        self,
        service: "QueueService",
        backend: "CloudTasksExecutionBackend",
        client: "FakeCloudTasksClient",
        execution_config: "CloudTasksExecutionConfig",
        events: "InMemoryQueueEventSink",
    ) -> "None":
        self.service = service
        self.backend = backend
        self.client = client
        self.execution_config = execution_config
        self.events = events

    async def enqueue(self, **overrides: "Any") -> "QueuedTaskRecord":
        """Persist one Cloud Tasks record without going through scheduling.

        Returns:
            The persisted record.
        """
        return await self.service.get_queue_backend().enqueue(TASK_NAME, execution_backend="cloudtasks", **overrides)

    async def reload(self, record: "QueuedTaskRecord") -> "QueuedTaskRecord":
        """Read a record back from storage.

        Returns:
            The stored record.
        """
        current = await self.service.get_queue_backend().get_task(record.id)
        assert current is not None
        return current

    async def schedule(self, record: "QueuedTaskRecord") -> "str | None":
        """Hand a record to the Cloud Tasks backend.

        Returns:
            The created delivery name.
        """
        return await self.backend.schedule(self.service, record)


@pytest.fixture
async def harness(
    shared_storage: "str", cloud_tasks_config: "Callable[..., CloudTasksExecutionConfig]"
) -> "AsyncIterator[Callable[..., Any]]":
    """Build Cloud Tasks harnesses that close with the test.

    Yields:
        A factory taking Cloud Tasks config overrides.
    """
    from litestar_queues.execution.cloudtasks import CloudTasksExecutionBackend

    opened: "list[QueueService]" = []

    async def build(**config_overrides: "Any") -> "Harness":
        queue_namespace = config_overrides.pop("queue_namespace", "litestar_queues")
        execution_config = cloud_tasks_config(**config_overrides)
        client = FakeCloudTasksClient()
        events = InMemoryQueueEventSink()
        queue_config = QueueConfig(
            namespace=queue_namespace,
            queue_backend=shared_storage,
            execution_backend=execution_config,
            worker=WorkerConfig(placement="external"),
            # Unbuffered: these tests assert on what one call published, and
            # the default producer buffer only flushes at close.
            events=QueueEventsConfig(delivery=EventDeliveryConfig(sinks=(events,), buffer=None)),
        )
        backend = CloudTasksExecutionBackend(config=queue_config, execution_config=execution_config, client=client)
        service = QueueService(queue_config, execution_backend=backend)
        await service.open()
        opened.append(service)
        return Harness(service, backend, client, execution_config, events)

    yield build

    for service in opened:
        await service.close()


# --------------------------------------------------------------------------- request shape


async def test_the_task_is_created_under_the_configured_queue_path(harness: "Callable[..., Any]") -> "None":
    live = await harness()
    record = await live.enqueue()

    await live.schedule(record)

    assert live.client.create_calls[0].parent == live.execution_config.queue_path


async def test_the_request_body_is_only_a_version_and_a_task_id(harness: "Callable[..., Any]") -> "None":
    """Byte equality, not a subset check: an extra field is a leak, not a nicety."""
    live = await harness()
    record = await live.enqueue()

    await live.schedule(record)

    expected = b'{"version":1,"task_id":"' + str(record.id).encode() + b'"}'
    assert live.client.create_calls[0].body == expected


async def test_no_task_identity_or_payload_reaches_the_request(harness: "Callable[..., Any]") -> "None":
    """Args, kwargs, metadata, the uniqueness key, and the task name all stay home.

    The consumer re-reads the live record by id, so anything else in the request
    is duplicated state that can go stale and be read by anyone with queue
    visibility.
    """
    live = await harness()
    record = await live.enqueue(
        args=("positional-material",),
        kwargs={"credential": "keyword-material"},
        metadata={"timeout": 60.0, "description": "metadata-material"},
        key="identity-material",
    )

    await live.schedule(record)

    serialized = repr(live.client.create_calls[0].request)
    for secret in ("positional-material", "keyword-material", "metadata-material", "identity-material", TASK_NAME):
        assert secret not in serialized


async def test_the_request_body_does_not_grow_with_the_payload(harness: "Callable[..., Any]") -> "None":
    """Cloud Tasks caps a task at 100 KiB; dispatch-by-id makes the cap unreachable."""
    live = await harness()
    small = await live.enqueue()
    large = await live.enqueue(args=("x" * 200_000,), kwargs={"blob": "y" * 200_000})

    await live.schedule(small)
    await live.schedule(large)

    small_body, large_body = (call.body for call in live.client.create_calls)
    assert len(small_body) == len(large_body)
    assert len(large_body) < 1024


async def test_the_request_posts_json_to_the_configured_route(harness: "Callable[..., Any]") -> "None":
    live = await harness()
    record = await live.enqueue()

    await live.schedule(record)

    http_request = live.client.create_calls[0].http_request
    assert http_request["http_method"] == "POST"
    assert http_request["url"] == live.execution_config.target_url
    assert http_request["headers"] == {"Content-Type": "application/json"}


async def test_the_request_carries_an_oidc_token_for_the_configured_audience(harness: "Callable[..., Any]") -> "None":
    """The private consumer authenticates the caller; no shared secret is sent."""
    live = await harness()
    record = await live.enqueue()

    await live.schedule(record)

    token = live.client.create_calls[0].http_request["oidc_token"]
    assert token == {
        "service_account_email": live.execution_config.service_account_email,
        "audience": live.execution_config.audience,
    }


async def test_the_request_carries_the_records_schedule_time(harness: "Callable[..., Any]") -> "None":
    live = await harness()
    due = datetime.now(timezone.utc) + timedelta(minutes=30)
    record = await live.enqueue(scheduled_at=due)

    await live.schedule(record)

    assert live.client.create_calls[0].task["schedule_time"] == due


async def test_an_undated_record_carries_no_schedule_time(harness: "Callable[..., Any]") -> "None":
    """Omitting the field lets Google dispatch immediately on its own clock.

    Stamping a local ``now`` instead would push producer clock skew into the
    delivery time for no benefit.
    """
    live = await harness()
    record = await live.enqueue()

    await live.schedule(record)

    assert "schedule_time" not in live.client.create_calls[0].task


async def test_the_request_carries_the_configured_dispatch_deadline(harness: "Callable[..., Any]") -> "None":
    live = await harness()
    record = await live.enqueue()

    await live.schedule(record)

    assert live.client.create_calls[0].task["dispatch_deadline"] == timedelta(
        seconds=live.execution_config.dispatch_deadline
    )


async def test_the_create_call_uses_the_configured_api_timeout(harness: "Callable[..., Any]") -> "None":
    """A hung control-plane call must not outlive the enqueue that triggered it."""
    live = await harness(api_timeout=4.5)
    record = await live.enqueue()

    await live.schedule(record)

    assert live.client.create_calls[0].timeout == 4.5


# --------------------------------------------------------------------------- delivery identity


async def test_the_delivery_name_is_persisted_before_the_task_is_created(harness: "Callable[..., Any]") -> "None":
    """A create call that never answers still leaves a name repair can look up.

    Persisting afterwards would lose the only handle on an in-flight delivery
    exactly when the response is ambiguous.
    """
    live = await harness()
    record = await live.enqueue()
    observed: "list[str | None]" = []

    async def _capture(call: "Any") -> "None":
        current = await live.reload(record)
        observed.append(current.execution_ref)

    live.client.on_create = _capture

    await live.schedule(record)

    assert observed == [live.client.create_calls[0].name]


async def test_the_delivery_name_is_random_and_encodes_the_record_and_attempt(harness: "Callable[..., Any]") -> "None":
    """Sequential ids would let anyone with queue read access enumerate deliveries."""
    live = await harness()
    record = await live.enqueue()

    name = await live.schedule(record)

    prefix = f"{live.execution_config.queue_path}/tasks/lq-{record.id.hex}-r0-"
    assert name is not None
    assert name.startswith(prefix)
    suffix = name[len(prefix) :]
    assert len(suffix) == 32
    assert suffix != record.id.hex


async def test_the_delivery_name_prefix_derives_from_queue_namespace(harness: "Callable[..., Any]") -> "None":
    live = await harness(queue_namespace="dma")
    record = await live.enqueue()

    name = await live.schedule(record)

    assert name is not None
    assert name.startswith(f"{live.execution_config.queue_path}/tasks/dma-{record.id.hex}-r0-")


async def test_two_records_never_share_a_delivery_name(harness: "Callable[..., Any]") -> "None":
    live = await harness()
    first = await live.enqueue()
    second = await live.enqueue()

    assert await live.schedule(first) != await live.schedule(second)


async def test_a_repeated_schedule_reuses_the_persisted_delivery_name(harness: "Callable[..., Any]") -> "None":
    """Retrying the same attempt must not create a second delivery of one record."""
    live = await harness()
    record = await live.enqueue()

    first = await live.schedule(record)
    second = await live.schedule(await live.reload(record))

    assert first == second


async def test_a_later_attempt_takes_a_new_delivery_name(harness: "Callable[..., Any]") -> "None":
    """Cloud Tasks keeps a deleted task's name reserved, so a retry needs a fresh one."""
    live = await harness()
    queue_backend = live.service.get_queue_backend()
    record = await live.enqueue(max_retries=2)

    first = await live.schedule(record)
    claimed, _ = await queue_backend.claim_task_with_expired(record.id)
    assert claimed is not None
    retried = await queue_backend.fail_task(record.id, "boom", expected_retry_count=0)
    assert retried is not None and retried.retry_count == 1

    second = await live.schedule(retried)

    assert second != first
    assert second is not None
    assert f"lq-{record.id.hex}-r1-" in second


# --------------------------------------------------------------------------- outcomes


async def test_schedule_returns_the_created_delivery_name(harness: "Callable[..., Any]") -> "None":
    live = await harness()
    record = await live.enqueue()

    name = await live.schedule(record)

    assert name == live.client.create_calls[0].name
    assert (await live.reload(record)).execution_ref == name


async def test_an_already_created_delivery_is_accepted(harness: "Callable[..., Any]") -> "None":
    """A retried create that collides on its own name already achieved its goal."""
    live = await harness()
    record = await live.enqueue()

    async def _collide(call: "Any") -> "None":
        msg = "task already exists"
        raise AlreadyExists(msg)

    live.client.on_create = _collide

    name = await live.schedule(record)

    assert name == (await live.reload(record)).execution_ref


async def test_an_already_exists_for_a_superseded_name_is_a_failure(harness: "Callable[..., Any]") -> "None":
    """The collision is only proof of success when it names this record's delivery.

    If another writer moved the record on, this call knows nothing about what is
    actually in flight and must not report success.
    """
    live = await harness()
    record = await live.enqueue()

    async def _collide_after_supersede(call: "Any") -> "None":
        await live.service.get_queue_backend().set_execution_ref(
            record.id, "cloudtasks", f"{live.execution_config.queue_path}/tasks/lq-elsewhere"
        )
        msg = "task already exists"
        raise AlreadyExists(msg)

    live.client.on_create = _collide_after_supersede

    with pytest.raises(QueueDispatchError):
        await live.schedule(record)


async def test_a_lost_response_raises_a_committed_dispatch_error(harness: "Callable[..., Any]") -> "None":
    """The record is durable, so the caller must not retry the whole enqueue."""
    live = await harness()
    record = await live.enqueue()

    async def _unavailable(call: "Any") -> "None":
        msg = "backend unavailable"
        raise ServiceUnavailable(msg)

    live.client.on_create = _unavailable

    with pytest.raises(QueueDispatchError) as excinfo:
        await live.schedule(record)

    assert excinfo.value.task_id == record.id
    assert excinfo.value.committed is True


async def test_a_failed_create_keeps_the_delivery_name_for_repair(harness: "Callable[..., Any]") -> "None":
    live = await harness()
    record = await live.enqueue()

    async def _unavailable(call: "Any") -> "None":
        msg = "backend unavailable"
        raise ServiceUnavailable(msg)

    live.client.on_create = _unavailable

    with pytest.raises(QueueDispatchError):
        await live.schedule(record)

    assert (await live.reload(record)).execution_ref == live.client.create_calls[0].name


async def test_the_dispatch_error_leaks_no_transport_detail(harness: "Callable[..., Any]") -> "None":
    """Google error text routinely quotes the URL and the caller identity.

    That message reaches application logs and, through the enqueue caller, can
    reach an HTTP response, so only the record id survives sanitization.
    """
    live = await harness()
    record = await live.enqueue()

    async def _leaky(call: "Any") -> "None":
        msg = (
            f"403 calling {live.execution_config.target_url} as "
            f"{live.execution_config.service_account_email}: token ya29.leaked-credential"
        )
        raise ServiceUnavailable(msg)

    live.client.on_create = _leaky

    with pytest.raises(QueueDispatchError) as excinfo:
        await live.schedule(record)

    message = str(excinfo.value)
    assert str(record.id) in message
    for secret in (
        live.execution_config.target_url,
        live.execution_config.service_account_email,
        live.execution_config.audience,
        "ya29.leaked-credential",
    ):
        assert secret is not None
        assert secret not in message
    assert isinstance(excinfo.value.__cause__, ServiceUnavailable)


async def test_a_failed_create_publishes_one_sanitized_event(harness: "Callable[..., Any]") -> "None":
    live = await harness()
    record = await live.enqueue()

    async def _leaky(call: "Any") -> "None":
        msg = f"503 calling {live.execution_config.target_url}: ya29.leaked-credential"
        raise ServiceUnavailable(msg)

    live.client.on_create = _leaky

    with pytest.raises(QueueDispatchError):
        await live.schedule(record)

    failures = [
        event
        for event in live.events.events
        if event.type == "task.event" and event.payload.get("phase") == "cloudtasks.schedule_failed"
    ]
    assert len(failures) == 1
    assert failures[0].task_id == str(record.id)
    assert "ya29.leaked-credential" not in repr(failures[0].payload)


async def test_a_terminal_record_is_never_delivered(harness: "Callable[..., Any]") -> "None":
    """A record cancelled between commit and schedule must not start running remotely."""
    live = await harness()
    record = await live.enqueue()
    await live.service.get_queue_backend().cancel_task(record.id)

    assert await live.schedule(record) is None
    assert live.client.create_calls == []


async def test_a_record_absent_from_storage_is_never_delivered(harness: "Callable[..., Any]") -> "None":
    """Scheduling reads the record back rather than trusting the caller's copy.

    Delivering an id storage does not hold would hand the consumer something it
    can never resolve, and it would retry until the delivery expired.
    """
    from litestar_queues.models import QueuedTaskRecord

    live = await harness()
    never_persisted = QueuedTaskRecord(task_name=TASK_NAME, execution_backend="cloudtasks")

    assert await live.schedule(never_persisted) is None
    assert live.client.create_calls == []


async def test_a_record_beyond_the_schedule_horizon_is_refused(harness: "Callable[..., Any]") -> "None":
    """Cloud Tasks rejects a schedule past thirty days; failing here keeps the reason legible."""
    live = await harness()
    record = await live.enqueue(scheduled_at=datetime.now(timezone.utc) + timedelta(days=31))

    with pytest.raises(QueueConfigurationError):
        await live.schedule(record)

    assert live.client.create_calls == []


async def test_a_record_naming_another_backend_is_refused(harness: "Callable[..., Any]") -> "None":
    live = await harness()
    record = await live.enqueue()
    await live.service.get_queue_backend().set_execution_backend(record.id, "local")

    with pytest.raises(QueueConfigurationError):
        await live.schedule(await live.reload(record))

    assert live.client.create_calls == []


# --------------------------------------------------------------------------- client lifecycle


async def test_close_releases_a_client_the_backend_created(monkeypatch: "pytest.MonkeyPatch") -> "None":
    """An owned transport leaks a connection pool per process if it is not closed."""
    from types import SimpleNamespace

    from litestar_queues.execution.cloudtasks import CloudTasksExecutionBackend
    from litestar_queues.execution.cloudtasks import backend as backend_module

    client = FakeCloudTasksClient()
    monkeypatch.setattr(
        backend_module, "import_module", lambda _: SimpleNamespace(CloudTasksAsyncClient=lambda: client)
    )
    backend = CloudTasksExecutionBackend()

    assert await backend._get_client() is client
    await backend.close()

    assert client.close_calls == 1


async def test_close_leaves_an_injected_client_to_its_owner() -> "None":
    """Whoever built the client controls its lifetime; this backend borrows it."""
    from litestar_queues.execution.cloudtasks import CloudTasksExecutionBackend

    client = FakeCloudTasksClient()

    await CloudTasksExecutionBackend(client=client).close()

    assert client.close_calls == 0

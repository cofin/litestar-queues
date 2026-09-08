"""What ``enqueue()`` owes a queue that has no worker watching it.

On a polled queue an ``enqueue()`` that returns is enough: some worker will find
the record eventually. A Cloud Tasks queue has no such reader, so the delivery
has to be created by the producer itself, and it has to be created in the one
order that cannot lose or duplicate work: persist first, name the delivery on
the record, then ask Google for it. A delivery that outran its record would
arrive at a consumer that cannot find the id it was handed.

The failure direction matters just as much. Once the record is committed the
caller must not retry the enqueue, so a creation failure has to surface as a
committed error over a record that is still there, still active, and still
carrying the delivery name repair will look for.
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any
from uuid import UUID

import pytest
from litestar.serialization import decode_json

from litestar_queues import QueueConfig, QueueService, WorkerConfig, task
from litestar_queues.exceptions import QueueDispatchError
from tests.unit.execution.cloudtasks._fakes import ServiceUnavailable

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable

    from litestar_queues.execution.cloudtasks import CloudTasksExecutionConfig
    from litestar_queues.models import QueuedTaskRecord
    from tests.unit.execution.cloudtasks._fakes import CreateCall, FakeCloudTasksClient

pytestmark = pytest.mark.anyio

TASK_NAME = "cloudtasks.enqueue_probe"
UNIQUE_TASK_NAME = "cloudtasks.enqueue_unique_probe"
UNIQUE_KEY = "enqueue-probe-identity"


@task(TASK_NAME)
async def probe(*args: "Any", **kwargs: "Any") -> "None":
    """A task whose only job is to be enqueued."""
    return None


@task(UNIQUE_TASK_NAME, key=UNIQUE_KEY, unique_until="forever")
async def unique_probe() -> "None":
    """A task that may exist exactly once, ever."""
    return None


class Harness:
    """A Cloud Tasks queue, its injected client, and a reader of the same store."""

    __slots__ = ("client", "reader", "service")

    def __init__(self, service: "QueueService", client: "FakeCloudTasksClient", reader: "QueueService") -> "None":
        self.service = service
        self.client = client
        self.reader = reader


@pytest.fixture
async def harness(
    shared_storage: "str", cloud_tasks_config: "Callable[..., CloudTasksExecutionConfig]"
) -> "AsyncIterator[Callable[..., Any]]":
    """Build Cloud Tasks enqueue harnesses that close with the test.

    The reader is a second service over the same store and is never opened: it
    stands in for the consumer process, which only ever reads records by id.

    Yields:
        A factory taking Cloud Tasks config overrides.
    """
    from litestar_queues.backends.factory import _queue_backend_registry
    from litestar_queues.execution.cloudtasks import CloudTasksExecutionBackend
    from tests.unit.execution.cloudtasks._fakes import FakeCloudTasksClient

    opened: "list[QueueService]" = []

    async def build(**config_overrides: "Any") -> "Harness":
        execution_config = cloud_tasks_config(**config_overrides)
        client = FakeCloudTasksClient()
        store = _queue_backend_registry[shared_storage]()

        def _config() -> "QueueConfig":
            return QueueConfig(
                queue_backend=shared_storage,
                execution_backend=execution_config,
                worker=WorkerConfig(placement="external"),
            )

        service = QueueService(
            _config(),
            queue_backend=store,
            execution_backend=CloudTasksExecutionBackend(execution_config=execution_config, client=client),
        )
        await service.open()
        opened.append(service)
        return Harness(service, client, QueueService(_config(), queue_backend=store))

    yield build

    for service in opened:
        await service.close()


# --------------------------------------------------------------------------- delivery on commit


async def test_a_committed_record_gets_exactly_one_delivery(harness: "Callable[..., Any]") -> "None":
    live = await harness()

    result = await live.service.enqueue(probe)

    assert len(live.client.create_calls) == 1
    assert live.client.create_calls[0].body == b'{"version":1,"task_id":"' + str(result.id).encode() + b'"}'


async def test_the_record_is_readable_elsewhere_before_the_delivery_exists(harness: "Callable[..., Any]") -> "None":
    """The consumer may be invoked the instant Google accepts the task.

    If creation could win the race against the write, the consumer would be
    handed an id its own store has never heard of and would have no way to tell
    that from a record someone deleted.
    """
    live = await harness()
    seen: "list[QueuedTaskRecord | None]" = []

    async def observe(call: "CreateCall") -> "None":
        # Resolve the id exactly as the consumer will: read it off the transport.
        seen.append(await live.reader.get_task(UUID(decode_json(call.body)["task_id"])))

    live.client.on_create = observe

    result = await live.service.enqueue(probe)

    assert seen and seen[0] is not None
    assert seen[0].id == result.id
    assert seen[0].is_terminal is False
    assert seen[0].execution_ref == live.client.create_calls[0].name


async def test_the_returned_handle_carries_the_persisted_delivery_reference(harness: "Callable[..., Any]") -> "None":
    """The caller's copy has to be the refreshed one, not the pre-schedule record."""
    live = await harness()

    result = await live.service.enqueue(probe)

    assert result.record is not None
    assert result.record.execution_ref == live.client.create_calls[0].name


# --------------------------------------------------------------------------- creation failure


async def test_a_failed_delivery_raises_a_committed_dispatch_error(harness: "Callable[..., Any]") -> "None":
    """``committed`` is the whole signal: the caller must not enqueue again."""
    live = await harness()

    async def fail(call: "CreateCall") -> "None":
        del call
        msg = "backend unavailable"
        raise ServiceUnavailable(msg)

    live.client.on_create = fail

    with pytest.raises(QueueDispatchError) as excinfo:
        await live.service.enqueue(probe)

    assert excinfo.value.committed is True
    assert excinfo.value.task_id is not None


async def test_a_failed_delivery_leaves_one_active_record_pointing_at_its_delivery(
    harness: "Callable[..., Any]",
) -> "None":
    """Repair needs the name that was attempted; a second record would double-run."""
    live = await harness()

    async def fail(call: "CreateCall") -> "None":
        del call
        msg = "backend unavailable"
        raise ServiceUnavailable(msg)

    live.client.on_create = fail

    with pytest.raises(QueueDispatchError) as excinfo:
        await live.service.enqueue(probe)

    assert (await live.service.get_queue_backend().get_statistics()).total == 1
    record = await live.service.get_task(excinfo.value.task_id)
    assert record is not None
    assert record.is_terminal is False
    assert record.execution_ref == live.client.create_calls[0].name


async def test_a_failed_delivery_keeps_the_identity_it_reserved(harness: "Callable[..., Any]") -> "None":
    """The record is committed, so releasing its key would let a duplicate in."""
    live = await harness()

    async def fail(call: "CreateCall") -> "None":
        del call
        msg = "backend unavailable"
        raise ServiceUnavailable(msg)

    live.client.on_create = fail

    with pytest.raises(QueueDispatchError) as excinfo:
        await live.service.enqueue(unique_probe)

    live.client.on_create = None
    again = await live.service.enqueue(unique_probe)

    assert again.id == excinfo.value.task_id
    assert (await live.service.get_queue_backend().get_statistics()).total == 1


@pytest.mark.parametrize("failure_point", ["client", "reservation", "request", "storage_read"])
async def test_post_commit_dispatch_failures_preserve_identity(
    harness: "Callable[..., Any]", monkeypatch: "pytest.MonkeyPatch", failure_point: "str"
) -> "None":
    """Every dispatch stage fails with a durable identity, even if its diagnostic sink fails."""
    from litestar_queues.execution.cloudtasks import backend as backend_module

    live = await harness()
    backend = live.service.get_execution_backend()
    storage_type = type(live.service.get_queue_backend())
    original_get_task = storage_type.get_task

    async def fail_async(*_args: "Any", **_kwargs: "Any") -> "Any":
        msg = "secret-target-and-credential"
        raise RuntimeError(msg)

    def fail_request(*_args: "Any", **_kwargs: "Any") -> "Any":
        msg = "secret-target-and-credential"
        raise RuntimeError(msg)

    if failure_point == "storage_read":
        monkeypatch.setattr(storage_type, "get_task", fail_async)
    elif failure_point == "request":
        monkeypatch.setattr(backend_module, "_create_task_request", fail_request)
    else:
        method = "_get_client" if failure_point == "client" else "_reserve_delivery_name"
        monkeypatch.setattr(type(backend), method, fail_async)
    monkeypatch.setattr(type(live.service.get_event_publisher()), "publish", fail_async)

    with pytest.raises(QueueDispatchError) as excinfo:
        await live.service.enqueue(probe)

    error = excinfo.value
    assert error.committed is True
    assert "secret-target-and-credential" not in str(error)
    assert error.task_id is not None
    monkeypatch.setattr(storage_type, "get_task", original_get_task)
    record = await live.reader.get_task(error.task_id)
    assert record is not None and record.status == "pending"
    assert (await live.service.get_queue_backend().get_statistics()).total == 1
    assert live.client.create_calls == []


@pytest.mark.parametrize("failure_point", ["client", "reservation", "create"])
async def test_post_commit_dispatch_cancellation_preserves_record(
    harness: "Callable[..., Any]", monkeypatch: "pytest.MonkeyPatch", failure_point: "str"
) -> "None":
    """Cancellation keeps its control-flow meaning after persistence."""
    live = await harness()

    async def cancel(*_args: "Any", **_kwargs: "Any") -> "Any":
        raise asyncio.CancelledError

    if failure_point == "create":
        live.client.on_create = cancel
    else:
        method = "_get_client" if failure_point == "client" else "_reserve_delivery_name"
        monkeypatch.setattr(type(live.service.get_execution_backend()), method, cancel)
    with pytest.raises(asyncio.CancelledError):
        await live.service.enqueue(unique_probe)
    record = await live.service.get_queue_backend().get_task_by_key(UNIQUE_KEY)
    assert record is not None and record.status == "pending"
    assert (await live.service.get_queue_backend().get_statistics()).total == 1


async def test_post_commit_result_read_preserves_identity(
    harness: "Callable[..., Any]", monkeypatch: "pytest.MonkeyPatch"
) -> "None":
    """Losing the final reload does not invite enqueueing an already delivered job again."""
    live = await harness()
    storage_type = type(live.service.get_queue_backend())
    original_get_task = storage_type.get_task

    async def fail_after_delivery(self: "Any", task_id: "UUID") -> "Any":
        if live.client.create_calls:
            msg = "result read unavailable"
            raise RuntimeError(msg)
        return await original_get_task(self, task_id)

    monkeypatch.setattr(storage_type, "get_task", fail_after_delivery)
    with pytest.raises(QueueDispatchError) as excinfo:
        await live.service.enqueue(probe)
    assert excinfo.value.committed is True
    assert excinfo.value.task_id is not None
    assert len(live.client.create_calls) == 1
    assert decode_json(live.client.create_calls[0].body)["task_id"] == str(excinfo.value.task_id)
    monkeypatch.setattr(storage_type, "get_task", original_get_task)
    assert await live.reader.get_task(excinfo.value.task_id) is not None


@pytest.mark.parametrize("failure_point", ["status", "end_span", "report", "cancel"])
async def test_fallback_logging_preserves_committed_dispatch_failure(
    harness: "Callable[..., Any]", monkeypatch: "pytest.MonkeyPatch", failure_point: "str"
) -> "None":
    from litestar_queues.execution.cloudtasks import backend as backend_module

    class RaisingHandler(logging.Handler):
        def emit(self, record: "logging.LogRecord") -> "None":
            msg = "diagnostic log unavailable"
            raise OSError(msg)

    live = await harness()
    record = await live.service.get_queue_backend().enqueue(
        TASK_NAME, execution_backend="cloudtasks", metadata={"timeout": 1740}
    )
    backend = live.service.get_execution_backend()
    runtime_type = type(live.service.observability_runtime)
    span = object()
    monkeypatch.setattr(runtime_type, "start_span", lambda *_args, **_kwargs: span)
    monkeypatch.setattr(runtime_type, "set_status_error", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime_type, "end_span", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(backend_module.logger, "handlers", [RaisingHandler()])
    monkeypatch.setattr(backend_module.logger, "propagate", False)
    monkeypatch.setattr(backend_module.logger, "level", logging.WARNING)

    def fail_diagnostic(*_args: "Any", **_kwargs: "Any") -> "None":
        msg = "telemetry unavailable"
        raise RuntimeError(msg)

    async def fail_publication(*_args: "Any", **_kwargs: "Any") -> "None":
        fail_diagnostic()

    primary: BaseException
    if failure_point == "cancel":
        primary = asyncio.CancelledError("dispatch cancelled")
    elif failure_point == "report":
        primary = ServiceUnavailable("provider unavailable")
    else:
        primary = QueueDispatchError("dispatch failed", task_id=record.id, committed=True)

    async def fail_dispatch(*_args: "Any", **_kwargs: "Any") -> "None":
        raise primary

    if failure_point == "report":
        live.client.on_create = fail_dispatch
        monkeypatch.setattr(type(backend), "_publish_delivery_failure", fail_publication)
    else:
        monkeypatch.setattr(type(backend), "_schedule_delivery", fail_dispatch)
        method = "set_status_error" if failure_point == "status" else "end_span"
        monkeypatch.setattr(runtime_type, method, fail_diagnostic)

    expected = asyncio.CancelledError if failure_point == "cancel" else QueueDispatchError
    with pytest.raises(expected) as excinfo:
        await backend.schedule(live.service, record)
    if failure_point == "report":
        assert excinfo.value.__cause__ is primary
    else:
        assert excinfo.value is primary
    if isinstance(excinfo.value, QueueDispatchError):
        assert excinfo.value.task_id == record.id
        assert excinfo.value.committed is True
    assert await live.reader.get_task(record.id) is not None


# --------------------------------------------------------------------------- records never delivered


async def test_a_record_that_expires_on_enqueue_is_never_delivered(harness: "Callable[..., Any]") -> "None":
    """Expiry runs before scheduling, so Google is never asked to deliver it."""
    live = await harness()

    result = await live.service.enqueue(probe, expires_at=datetime.now(timezone.utc) - timedelta(seconds=1))

    assert live.client.create_calls == []
    assert result.record is not None
    assert result.record.status == "expired"


async def test_a_deduplicated_enqueue_creates_no_second_delivery(harness: "Callable[..., Any]") -> "None":
    """A forever-unique key returns the existing handle without touching Google."""
    live = await harness()
    first = await live.service.enqueue(unique_probe)

    second = await live.service.enqueue(unique_probe)

    assert second.id == first.id
    assert len(live.client.create_calls) == 1

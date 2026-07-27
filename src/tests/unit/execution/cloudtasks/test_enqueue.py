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

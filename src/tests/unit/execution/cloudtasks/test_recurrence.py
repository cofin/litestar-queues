"""Every other way a record becomes due on a queue with no worker.

Ordinary enqueue is the obvious path, but three more create work that nothing
would ever deliver if they were left unwired: startup registers the first
occurrence of a recurring schedule, completing one occurrence writes the next,
and a failed attempt with retries left puts the record back into pending. On a
polled queue a worker notices all three. Here the process that wrote the record
is the only one that can hand it to the transport.

The horizon is what makes recurrence different from enqueue. An interval longer
than Cloud Tasks will hold a delivery is not a bad call to reject at the API --
it is a schedule that cannot run, so it has to stop where it is created, and
say so.
"""

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

import pytest

from litestar_queues import QueueConfig, QueueService, WorkerConfig, task
from litestar_queues.events import EventDeliveryConfig, InMemoryQueueEventSink, QueueEventsConfig
from litestar_queues.exceptions import QueueConfigurationError, QueueDispatchError
from litestar_queues.task import clear_task_registry, get_scheduled_tasks
from tests.unit.execution.cloudtasks._fakes import ServiceUnavailable

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable

    from litestar_queues.execution.cloudtasks import CloudTasksExecutionConfig
    from litestar_queues.models import QueuedTaskRecord
    from tests.unit.execution.cloudtasks._fakes import CreateCall, FakeCloudTasksClient

pytestmark = pytest.mark.anyio

BEYOND_HORIZON = timedelta(days=40)
REJECTED_PHASE = "cloudtasks.schedule_rejected"


class Harness:
    """A Cloud Tasks queue with an injected client and an unbuffered event sink."""

    __slots__ = ("client", "events", "service")

    def __init__(
        self, service: "QueueService", client: "FakeCloudTasksClient", events: "InMemoryQueueEventSink"
    ) -> "None":
        self.service = service
        self.client = client
        self.events = events

    def deliveries_for(self, record: "QueuedTaskRecord") -> "list[str]":
        """Names of every delivery created for one record.

        Returns:
            The created delivery names, in creation order.
        """
        return [call.name for call in self.client.create_calls if f"lq-{record.id.hex}-" in call.name]

    async def claim(self, record: "QueuedTaskRecord") -> "QueuedTaskRecord":
        """Claim a record regardless of when it is due.

        Returns:
            The claimed record.
        """
        claimed = await self.service.get_queue_backend().claim_task(record.id)
        assert claimed is not None
        return claimed


@pytest.fixture(autouse=True)
def _clean_registry() -> "None":
    """Schedules live in a process-global registry, so each test starts empty."""
    clear_task_registry()


@pytest.fixture
async def harness(
    shared_storage: "str", cloud_tasks_config: "Callable[..., CloudTasksExecutionConfig]"
) -> "AsyncIterator[Callable[..., Any]]":
    """Build Cloud Tasks recurrence harnesses that close with the test.

    Yields:
        A factory taking Cloud Tasks config overrides.
    """
    from litestar_queues.execution.cloudtasks import CloudTasksExecutionBackend
    from tests.unit.execution.cloudtasks._fakes import FakeCloudTasksClient

    opened: "list[QueueService]" = []

    async def build(**config_overrides: "Any") -> "Harness":
        execution_config = cloud_tasks_config(**config_overrides)
        client = FakeCloudTasksClient()
        events = InMemoryQueueEventSink()
        service = QueueService(
            QueueConfig(
                queue_backend=shared_storage,
                execution_backend=execution_config,
                worker=WorkerConfig(placement="external"),
                # Unbuffered: the rejection assertions read what one call published.
                events=QueueEventsConfig(delivery=EventDeliveryConfig(sinks=(events,), buffer=None)),
            ),
            execution_backend=CloudTasksExecutionBackend(execution_config=execution_config, client=client),
        )
        await service.open()
        opened.append(service)
        return Harness(service, client, events)

    yield build

    for service in opened:
        await service.close()


# --------------------------------------------------------------------------- startup


async def test_startup_delivers_the_first_occurrence(harness: "Callable[..., Any]") -> "None":
    """External placement means no worker will ever notice this record on its own."""

    @task("cloudtasks.recurrence.first", interval=60)
    async def probe() -> "None":
        return None

    live = await harness()

    records = await live.service.initialize_schedules()

    assert len(records) == 1
    assert live.deliveries_for(records[0]) == [live.client.create_calls[0].name]


async def test_an_unchanged_schedule_is_reused_without_a_second_delivery(harness: "Callable[..., Any]") -> "None":
    """Restarting a process must not double-deliver the occurrence already in flight."""

    @task("cloudtasks.recurrence.unchanged", interval=60)
    async def probe() -> "None":
        return None

    live = await harness()
    first = await live.service.initialize_schedules()

    second = await live.service.initialize_schedules()

    assert second[0].id == first[0].id
    assert len(live.client.create_calls) == 1


async def test_a_changed_schedule_cancels_the_old_occurrence_and_delivers_the_new_one(
    harness: "Callable[..., Any]",
) -> "None":
    live = await harness()

    @task("cloudtasks.recurrence.changed", interval=60)
    async def probe() -> "None":
        return None

    stale = await live.service.get_queue_backend().enqueue(
        "cloudtasks.recurrence.changed",
        key="scheduled:cloudtasks.recurrence.changed",
        execution_backend="cloudtasks",
        scheduled_at=datetime.now(timezone.utc) + timedelta(seconds=600),
        metadata={
            "schedule": {**get_scheduled_tasks()["cloudtasks.recurrence.changed"].as_metadata(), "interval": 999.0}
        },
    )

    records = await live.service.initialize_schedules()

    assert records[0].id != stale.id
    assert (await live.service.get_task(stale.id)).status == "cancelled"
    assert live.deliveries_for(records[0]) != []
    assert live.deliveries_for(stale) == []


async def test_a_first_occurrence_beyond_the_horizon_is_refused_before_anything_is_written(
    harness: "Callable[..., Any]",
) -> "None":
    """Startup is the operator's feedback loop, so this fails loudly rather than quietly."""

    @task("cloudtasks.recurrence.far_first", interval=BEYOND_HORIZON)
    async def probe() -> "None":
        return None

    live = await harness()

    with pytest.raises(QueueConfigurationError):
        await live.service.initialize_schedules()

    assert live.client.create_calls == []
    assert (await live.service.get_queue_backend().get_statistics()).total == 0


# --------------------------------------------------------------------------- recurrence


async def test_completing_an_occurrence_delivers_the_next_one(harness: "Callable[..., Any]") -> "None":
    @task("cloudtasks.recurrence.next", interval=60)
    async def probe() -> "None":
        return None

    live = await harness()
    # Enqueued due-now rather than through startup: the next occurrence is what
    # this covers, and a schedule's first run is always in the future.
    first = await live.service.get_queue_backend().enqueue(
        "cloudtasks.recurrence.next",
        key="scheduled:cloudtasks.recurrence.next",
        execution_backend="cloudtasks",
        metadata={"schedule": get_scheduled_tasks()["cloudtasks.recurrence.next"].as_metadata()},
    )

    await live.service.execute_record(await live.claim(first))

    following = await live.service.get_queue_backend().get_task_by_key("scheduled:cloudtasks.recurrence.next")
    assert following is not None
    assert following.id != first.id
    assert live.deliveries_for(following) != []


async def test_a_next_occurrence_beyond_the_horizon_stops_the_chain(harness: "Callable[..., Any]") -> "None":
    """The occurrence that ran stays completed; nothing undeliverable is written."""

    @task("cloudtasks.recurrence.far_next", interval=BEYOND_HORIZON)
    async def probe() -> "None":
        return None

    live = await harness()
    first = await live.service.get_queue_backend().enqueue(
        "cloudtasks.recurrence.far_next",
        key="scheduled:cloudtasks.recurrence.far_next",
        execution_backend="cloudtasks",
        metadata={"schedule": get_scheduled_tasks()["cloudtasks.recurrence.far_next"].as_metadata()},
    )

    completed = await live.service.execute_record(await live.claim(first))

    assert completed.status == "completed"
    assert (await live.service.get_queue_backend().get_statistics()).total == 1
    assert live.client.create_calls == []
    rejected = [event for event in live.events.events if event.payload.get("phase") == REJECTED_PHASE]
    assert len(rejected) == 1
    assert rejected[0].task_id == str(first.id)


async def test_a_rejected_recurrence_leaks_nothing_about_the_target(harness: "Callable[..., Any]") -> "None":
    """The event travels wherever sinks go, so it carries a phase and nothing else."""

    @task("cloudtasks.recurrence.far_quiet", interval=BEYOND_HORIZON)
    async def probe() -> "None":
        return None

    live = await harness()
    first = await live.service.get_queue_backend().enqueue(
        "cloudtasks.recurrence.far_quiet",
        key="scheduled:cloudtasks.recurrence.far_quiet",
        execution_backend="cloudtasks",
        metadata={"schedule": get_scheduled_tasks()["cloudtasks.recurrence.far_quiet"].as_metadata()},
    )

    await live.service.execute_record(await live.claim(first))

    rejected = [event for event in live.events.events if event.payload.get("phase") == REJECTED_PHASE]
    assert len(rejected) == 1
    serialized = repr(rejected[0])
    for secret in ("queues@example-project.iam.gserviceaccount.com", "queue-consumer-abcdef-uc.a.run.app"):
        assert secret not in serialized


# --------------------------------------------------------------------------- retry


async def test_a_retrying_record_gets_a_delivery_for_the_new_attempt(harness: "Callable[..., Any]") -> "None":
    """Nothing else would ever run attempt two: the queue has no poller."""

    @task("cloudtasks.recurrence.retry", retries=1)
    async def probe() -> "None":
        msg = "boom"
        raise RuntimeError(msg)

    live = await harness()
    result = await live.service.enqueue(probe)
    record = await live.service.get_task(result.id)

    updated = await live.service.execute_record(await live.claim(record))

    assert updated.status == "pending"
    assert updated.retry_count == 1
    names = live.deliveries_for(record)
    assert len(names) == 2
    assert f"lq-{record.id.hex}-r0-" in names[0]
    assert f"lq-{record.id.hex}-r1-" in names[1]


async def test_an_exhausted_retry_creates_no_further_delivery(harness: "Callable[..., Any]") -> "None":
    @task("cloudtasks.recurrence.exhausted", retries=0)
    async def probe() -> "None":
        msg = "boom"
        raise RuntimeError(msg)

    live = await harness()
    result = await live.service.enqueue(probe)
    record = await live.service.get_task(result.id)

    updated = await live.service.execute_record(await live.claim(record))

    assert updated.status == "failed"
    assert len(live.deliveries_for(record)) == 1


async def test_a_delivery_failure_after_a_retry_transition_reaches_the_caller(harness: "Callable[..., Any]") -> "None":
    """The consumer has to answer 503 so Cloud Tasks redelivers the attempt it holds."""

    @task("cloudtasks.recurrence.retry_undeliverable", retries=1)
    async def probe() -> "None":
        msg = "boom"
        raise RuntimeError(msg)

    live = await harness()
    result = await live.service.enqueue(probe)
    record = await live.service.get_task(result.id)

    async def fail_second(call: "CreateCall") -> "None":
        if len(live.client.create_calls) > 1:
            msg = "backend unavailable"
            raise ServiceUnavailable(msg)

    live.client.on_create = fail_second

    with pytest.raises(QueueDispatchError):
        await live.service.execute_record(await live.claim(record))

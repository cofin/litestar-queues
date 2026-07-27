"""Recreating deliveries the transport no longer holds.

A queue with no worker has a single point of failure the polled backends do not
have: if the Cloud Task disappears while its record stays active, nothing is
watching. The record sits pending forever. Deliveries go missing for ordinary
reasons -- a create call that errored after Google had already accepted it, an
operator purging the queue, a retention window closing on a task whose schedule
time had not arrived.

Repair reuses the existing bounded external-maintenance phase rather than adding
a fifth one, so the interesting property is arithmetic: one pass must never
examine more records than the phase's budget allows, no matter how the budget is
split between repair and ordinary reconciliation.
"""

from dataclasses import replace
from typing import TYPE_CHECKING, Any, cast

import pytest
from litestar.serialization import decode_json

from litestar_queues import QueueConfig, QueueService, WorkerConfig, task
from litestar_queues.events import EventDeliveryConfig, InMemoryQueueEventSink, QueueEventsConfig
from litestar_queues.execution.base import DispatchRepairResult
from litestar_queues.task import clear_task_registry
from tests.unit.execution.cloudtasks._fakes import ServiceUnavailable

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable

    from litestar_queues.execution.cloudtasks import CloudTasksExecutionBackend, CloudTasksExecutionConfig
    from litestar_queues.models import QueuedTaskRecord
    from tests.unit.execution.cloudtasks._fakes import CreateCall, FakeCloudTasksClient

pytestmark = pytest.mark.anyio

REPAIR_FAILED_PHASE = "cloudtasks.repair_failed"


class Harness:
    """A Cloud Tasks queue whose transport can be made to forget a delivery."""

    __slots__ = ("backend", "client", "events", "service")

    def __init__(
        self,
        service: "QueueService",
        backend: "CloudTasksExecutionBackend",
        client: "FakeCloudTasksClient",
        events: "InMemoryQueueEventSink",
    ) -> "None":
        self.service = service
        self.backend = backend
        self.client = client
        self.events = events

    async def enqueue(self, task_name: "str") -> "QueuedTaskRecord":
        """Enqueue one record and return it with its delivery reference.

        Returns:
            The persisted record.
        """
        result = await self.service.enqueue(task_name)
        record = await self.service.get_task(result.id)
        assert record is not None
        return record

    def forget_every_delivery(self) -> "None":
        """Drop every task the transport holds, leaving the records untouched."""
        self.client.existing.clear()

    async def repair(self, *, limit: "int") -> "DispatchRepairResult":
        """Run one bounded repair pass.

        Returns:
            The pass result.
        """
        return await self.backend.repair(self.service, limit=limit)

    def repair_failures(self) -> "list[Any]":
        """Published repair-failure events.

        Returns:
            Every event carrying the repair-failure phase.
        """
        return [event for event in self.events.events if event.payload.get("phase") == REPAIR_FAILED_PHASE]


@pytest.fixture(autouse=True)
def _clean_registry() -> "None":
    """Tasks live in a process-global registry, so each test starts empty."""
    clear_task_registry()


@pytest.fixture
async def harness(
    shared_storage: "str", cloud_tasks_config: "Callable[..., CloudTasksExecutionConfig]"
) -> "AsyncIterator[Callable[..., Any]]":
    """Build Cloud Tasks repair harnesses that close with the test.

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
        backend = CloudTasksExecutionBackend(execution_config=execution_config, client=client)
        service = QueueService(
            QueueConfig(
                queue_backend=shared_storage,
                execution_backend=execution_config,
                worker=WorkerConfig(placement="external"),
                # Unbuffered: the failure assertions read what one pass published.
                events=QueueEventsConfig(delivery=EventDeliveryConfig(sinks=(events,), buffer=None)),
            ),
            execution_backend=backend,
        )
        await service.open()
        opened.append(service)
        return Harness(service, backend, client, events)

    yield build

    for service in opened:
        await service.close()


def _register(name: "str") -> "None":
    """Register a trivial task under ``name``."""

    @task(name)
    async def probe() -> "None":
        return None


# --------------------------------------------------------------------------- repair


async def test_a_missing_delivery_is_recreated_under_a_new_name(harness: "Callable[..., Any]") -> "None":
    """Reusing the old name would collide with the tombstone Cloud Tasks keeps."""
    _register("cloudtasks.repair.missing")
    live = await harness()
    record = await live.enqueue("cloudtasks.repair.missing")
    original = record.execution_ref
    live.forget_every_delivery()

    outcome = await live.repair(limit=10)

    assert outcome == DispatchRepairResult(examined=1, changed=1)
    repaired = await live.service.get_task(record.id)
    assert repaired.execution_ref != original
    assert repaired.execution_ref in live.client.existing
    assert len(live.client.create_calls) == 2


async def test_a_delivery_the_transport_still_holds_is_left_alone(harness: "Callable[..., Any]") -> "None":
    _register("cloudtasks.repair.present")
    live = await harness()
    record = await live.enqueue("cloudtasks.repair.present")

    outcome = await live.repair(limit=10)

    assert outcome == DispatchRepairResult(examined=1, changed=0)
    assert live.client.get_calls == [record.execution_ref]
    assert len(live.client.create_calls) == 1


async def test_a_pass_never_examines_more_records_than_its_budget(harness: "Callable[..., Any]") -> "None":
    """The whole point of reusing the maintenance phase is that it stays finite."""
    live = await harness()
    for index in range(5):
        _register(f"cloudtasks.repair.budget{index}")
        await live.enqueue(f"cloudtasks.repair.budget{index}")
    live.forget_every_delivery()

    outcome = await live.repair(limit=2)

    assert outcome == DispatchRepairResult(examined=2, changed=2)
    assert len(live.client.get_calls) == 2


async def test_a_running_record_is_never_re_delivered(harness: "Callable[..., Any]") -> "None":
    """Running means the consumer has it and Cloud Tasks is holding the response open."""
    _register("cloudtasks.repair.running")
    live = await harness()
    record = await live.enqueue("cloudtasks.repair.running")
    assert await live.service.get_queue_backend().claim_task(record.id) is not None
    live.forget_every_delivery()

    outcome = await live.repair(limit=10)

    assert outcome == DispatchRepairResult(examined=1, changed=0)
    assert live.client.get_calls == []


async def test_a_record_that_went_terminal_before_repair_is_never_re_delivered(
    harness: "Callable[..., Any]", monkeypatch: "pytest.MonkeyPatch"
) -> "None":
    """The candidate list is a snapshot, so every candidate is re-read before use."""
    _register("cloudtasks.repair.cancelled")
    live = await harness()
    record = await live.enqueue("cloudtasks.repair.cancelled")
    # Detached while the record is still active: an in-process backend hands out
    # the live object, which would make the re-read impossible to observe.
    snapshot = replace(record)
    queue_backend = live.service.get_queue_backend()
    await queue_backend.cancel_task(record.id)
    live.forget_every_delivery()

    async def stale_listing(self: "Any", *, limit: "int | None" = None) -> "list[QueuedTaskRecord]":
        return [snapshot]

    monkeypatch.setattr(type(queue_backend), "list_running_external", stale_listing)

    outcome = await live.repair(limit=10)

    assert outcome == DispatchRepairResult(examined=1, changed=0)
    assert live.client.get_calls == []


async def test_a_record_owned_by_another_execution_backend_is_left_alone(harness: "Callable[..., Any]") -> "None":
    live = await harness()
    alien = await live.service.get_queue_backend().enqueue("tasks.alien", execution_backend="cloudrun")
    await live.service.get_queue_backend().set_execution_ref(alien.id, "cloudrun", "jobs/alien-123")

    outcome = await live.repair(limit=10)

    assert outcome == DispatchRepairResult(examined=1, changed=0)
    assert live.client.get_calls == []
    assert live.client.create_calls == []


async def test_a_candidate_is_attempted_once_per_pass(harness: "Callable[..., Any]") -> "None":
    """A queue whose target is broken must not spin inside one maintenance window."""
    _register("cloudtasks.repair.attempt_once")
    live = await harness()
    await live.enqueue("cloudtasks.repair.attempt_once")
    live.forget_every_delivery()

    async def refuse(call: "CreateCall") -> "None":
        msg = "backend unavailable"
        raise ServiceUnavailable(msg)

    live.client.on_create = refuse

    outcome = await live.repair(limit=10)

    assert outcome == DispatchRepairResult(examined=1, changed=0)
    assert len(live.client.create_calls) == 2


async def test_one_failing_candidate_does_not_end_the_pass(harness: "Callable[..., Any]") -> "None":
    _register("cloudtasks.repair.broken")
    _register("cloudtasks.repair.healthy")
    live = await harness()
    broken = await live.enqueue("cloudtasks.repair.broken")
    await live.enqueue("cloudtasks.repair.healthy")
    live.forget_every_delivery()

    async def refuse_broken(call: "CreateCall") -> "None":
        if decode_json(call.body)["task_id"] == str(broken.id):
            msg = "backend unavailable"
            raise ServiceUnavailable(msg)

    live.client.on_create = refuse_broken

    outcome = await live.repair(limit=10)

    assert outcome == DispatchRepairResult(examined=2, changed=1)


async def test_a_failed_repair_is_reported_once_without_the_target(harness: "Callable[..., Any]") -> "None":
    """The event travels wherever sinks go, so it carries a phase and nothing else."""
    _register("cloudtasks.repair.sanitized")
    live = await harness()
    await live.enqueue("cloudtasks.repair.sanitized")
    live.forget_every_delivery()

    async def refuse(call: "CreateCall") -> "None":
        msg = f"PERMISSION_DENIED on {live.backend.execution_config.target_url}"
        raise ServiceUnavailable(msg)

    live.client.on_create = refuse

    await live.repair(limit=10)

    failures = live.repair_failures()
    assert len(failures) == 1
    serialized = repr(failures[0])
    for secret in ("queues@example-project.iam.gserviceaccount.com", "queue-consumer-abcdef-uc.a.run.app"):
        assert secret not in serialized


async def test_a_lookup_failure_is_reported_and_creates_nothing(harness: "Callable[..., Any]") -> "None":
    """An error that is not "absent" leaves the delivery's fate unknown, so nothing is created."""
    _register("cloudtasks.repair.lookup")
    live = await harness()
    await live.enqueue("cloudtasks.repair.lookup")

    async def refuse_lookup(name: "str") -> "None":
        msg = "backend unavailable"
        raise ServiceUnavailable(msg)

    live.client.on_get = refuse_lookup

    outcome = await live.repair(limit=10)

    assert outcome == DispatchRepairResult(examined=1, changed=0)
    assert len(live.client.create_calls) == 1
    assert len(live.repair_failures()) == 1


# --------------------------------------------------------------------------- budget


async def test_an_unbounded_sweep_never_repairs(harness: "Callable[..., Any]") -> "None":
    """Repair is a maintenance budget; the worker's unbounded sweep has no ceiling to respect."""
    _register("cloudtasks.repair.unbounded")
    live = await harness()
    await live.enqueue("cloudtasks.repair.unbounded")
    live.forget_every_delivery()

    reconciled = await live.service.reconcile_external()

    assert reconciled == 0
    assert live.client.get_calls == []
    assert len(live.client.create_calls) == 1


async def test_a_bounded_sweep_hands_reconciliation_only_what_repair_left(
    harness: "Callable[..., Any]", monkeypatch: "pytest.MonkeyPatch"
) -> "None":
    live = await harness()
    for index in range(4):
        _register(f"cloudtasks.repair.share{index}")
        await live.enqueue(f"cloudtasks.repair.share{index}")
    live.forget_every_delivery()
    queue_backend = live.service.get_queue_backend()
    original = type(queue_backend).list_running_external
    budgets: "list[int | None]" = []

    async def spy(self: "Any", *, limit: "int | None" = None) -> "list[QueuedTaskRecord]":
        budgets.append(limit)
        return cast("list[QueuedTaskRecord]", await original(self, limit=limit))

    monkeypatch.setattr(type(queue_backend), "list_running_external", spy)

    changed = await live.service.reconcile_external(limit=3)

    assert changed == 3
    assert budgets == [3, 0]


async def test_a_partial_repair_leaves_the_rest_of_the_budget_for_reconciliation(
    harness: "Callable[..., Any]", monkeypatch: "pytest.MonkeyPatch"
) -> "None":
    _register("cloudtasks.repair.partial")
    live = await harness()
    await live.enqueue("cloudtasks.repair.partial")
    live.forget_every_delivery()
    queue_backend = live.service.get_queue_backend()
    original = type(queue_backend).list_running_external
    budgets: "list[int | None]" = []

    async def spy(self: "Any", *, limit: "int | None" = None) -> "list[QueuedTaskRecord]":
        budgets.append(limit)
        return cast("list[QueuedTaskRecord]", await original(self, limit=limit))

    monkeypatch.setattr(type(queue_backend), "list_running_external", spy)

    changed = await live.service.reconcile_external(limit=5)

    assert changed == 1
    assert budgets == [5, 4]


# --------------------------------------------------------------------------- polled backends


class _ForbiddenService:
    """Any attribute access means a polled backend went looking for work."""

    def __getattr__(self, name: "str") -> "Any":
        msg = f"a polled execution backend reached for service.{name} during repair"
        raise AssertionError(msg)


@pytest.mark.parametrize("backend_name", ["local", "immediate", "cloudrun"])
async def test_a_polled_backend_repairs_nothing(backend_name: "str") -> "None":
    """Nothing can go missing from a store the worker reads directly."""
    from litestar_queues.execution import get_execution_backend_class

    backend = get_execution_backend_class(backend_name)()

    assert await backend.repair(cast("Any", _ForbiddenService()), limit=10) == DispatchRepairResult()

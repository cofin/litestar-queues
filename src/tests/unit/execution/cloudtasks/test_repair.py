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

import asyncio
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, cast

import pytest
from litestar.serialization import decode_json

from litestar_queues import QueueConfig, QueueService, WorkerConfig, task
from litestar_queues.backends.base import DispatchRepairCandidates
from litestar_queues.events import EventDeliveryConfig, InMemoryQueueEventSink, QueueEventsConfig
from litestar_queues.exceptions import QueueConfigurationError
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


@pytest.mark.parametrize("future", [False, True])
async def test_null_ref_survives_a_fresh_service(harness: "Callable[..., Any]", future: "bool") -> "None":
    live = await harness()
    storage = live.service.get_queue_backend()
    record = await storage.enqueue(
        "cloudtasks.crash",
        execution_backend="cloudtasks",
        scheduled_at=datetime.now(timezone.utc) + timedelta(hours=1) if future else None,
    )
    fresh = QueueService(live.service.config, queue_backend=storage, execution_backend=live.backend)
    try:
        await fresh.open()
        result = await live.backend.repair(fresh, limit=1)
        assert result == DispatchRepairResult(examined=1, changed=1, limit_reached=True)
        assert len(live.client.create_calls) == 1
        assert decode_json(live.client.create_calls[0].body)["task_id"] == str(record.id)
    finally:
        await fresh.close()


@pytest.mark.parametrize("mode", ["schedule", "repair", "mixed"])
async def test_reservation_race_has_one_winner(
    harness: "Callable[..., Any]", monkeypatch: "pytest.MonkeyPatch", mode: "str"
) -> "None":
    live = await harness()
    storage = live.service.get_queue_backend()
    record = await storage.enqueue("cloudtasks.race", execution_backend="cloudtasks")
    original = type(storage).get_task
    arrived = 0
    gate = asyncio.Event()

    async def simultaneous_read(self: "Any", task_id: "Any") -> "Any":
        nonlocal arrived
        value = await original(self, task_id)
        if arrived < 2:
            arrived += 1
            snapshot = replace(value)
            if arrived == 2:
                gate.set()
            await gate.wait()
            return snapshot
        return value

    monkeypatch.setattr(type(storage), "get_task", simultaneous_read)
    first = live.repair(limit=1) if mode == "repair" else live.backend.schedule(live.service, record)
    second = live.backend.schedule(live.service, record) if mode == "schedule" else live.repair(limit=1)
    await asyncio.gather(first, second)
    assert len(live.client.create_calls) == 1


@pytest.mark.parametrize("change", ["cancel", "claim", "retry", "backend", "delete"])
async def test_lookup_cannot_reserve_a_changed_attempt(harness: "Callable[..., Any]", change: "str") -> "None":
    live = await harness()
    storage = live.service.get_queue_backend()
    record = await storage.enqueue("cloudtasks.changed", execution_backend="cloudtasks", max_retries=2)
    await live.backend.schedule(live.service, record)
    live.forget_every_delivery()

    async def change_during_lookup(name: "str") -> "None":
        if change == "cancel":
            await storage.cancel_task(record.id)
        elif change == "claim":
            await storage.claim_task(record.id)
        elif change == "retry":
            await storage.claim_task(record.id)
            await storage.fail_task(record.id, "retry", retry=True)
        elif change == "backend":
            await storage.set_execution_backend(record.id, "cloudrun")
        else:
            await storage.cancel_task(record.id)
            await storage.cleanup_terminal(datetime.now(timezone.utc) + timedelta(seconds=1))

    live.client.on_get = change_during_lookup
    result = await live.repair(limit=1)
    assert result == DispatchRepairResult(examined=1, unchanged=1, limit_reached=True)
    assert len(live.client.create_calls) == 1


async def test_zero_and_negative_repair_limits_do_no_work(harness: "Callable[..., Any]") -> "None":
    live = await harness()
    await live.service.get_queue_backend().enqueue("cloudtasks.zero", execution_backend="cloudtasks")
    assert await live.repair(limit=0) == DispatchRepairResult(examined=0, changed=0)
    with pytest.raises(QueueConfigurationError, match="non-negative"):
        await live.repair(limit=-1)
    assert live.client.get_calls == []
    assert live.client.create_calls == []


async def test_stale_page_counts_raw_examined(
    harness: "Callable[..., Any]", monkeypatch: "pytest.MonkeyPatch"
) -> "None":
    live = await harness()

    async def stale_page(self: "Any", execution_backend: "str", *, limit: "int") -> "DispatchRepairCandidates":
        return DispatchRepairCandidates(examined=limit, limit_reached=True)

    monkeypatch.setattr(type(live.service.get_queue_backend()), "list_dispatch_repair_candidates", stale_page)
    assert await live.repair(limit=3) == DispatchRepairResult(examined=3, unchanged=3, limit_reached=True)


async def test_page_attempt_snapshots_survive_an_earlier_candidate_await(harness: "Callable[..., Any]") -> "None":
    live = await harness()
    storage = live.service.get_queue_backend()
    first = await storage.enqueue("cloudtasks.first", execution_backend="cloudtasks")
    second = await storage.enqueue("cloudtasks.second", execution_backend="cloudtasks", max_retries=2)

    async def advance_second_attempt(call: "CreateCall") -> "None":
        assert decode_json(call.body)["task_id"] == str(first.id)
        await storage.claim_task(second.id)
        await storage.fail_task(second.id, "retry", retry=True)

    live.client.on_create = advance_second_attempt
    assert await live.repair(limit=2) == DispatchRepairResult(examined=2, changed=1, unchanged=1, limit_reached=True)
    assert len(live.client.create_calls) == 1


async def test_healthy_prefix_rotates_across_execution_instances(harness: "Callable[..., Any]") -> "None":
    from litestar_queues.execution.cloudtasks import CloudTasksExecutionBackend

    live = await harness()
    storage = live.service.get_queue_backend()
    healthy = await storage.enqueue("cloudtasks.healthy", execution_backend="cloudtasks")
    await live.backend.schedule(live.service, healthy)
    missing = await storage.enqueue("cloudtasks.missing", execution_backend="cloudtasks")
    assert await live.repair(limit=1) == DispatchRepairResult(examined=1, unchanged=1, limit_reached=True)
    fresh = CloudTasksExecutionBackend(execution_config=live.backend.execution_config, client=live.client)
    fresh_service = QueueService(live.service.config, queue_backend=storage, execution_backend=fresh)
    try:
        await fresh_service.open()
        assert await fresh.repair(fresh_service, limit=1) == DispatchRepairResult(
            examined=1, changed=1, limit_reached=True
        )
    finally:
        await fresh_service.close()
    assert decode_json(live.client.create_calls[-1].body)["task_id"] == str(missing.id)


@pytest.mark.parametrize("change", ["cancel", "claim", "retry", "backend", "expire", "delete", "interrupt"])
async def test_repair_rechecks_ownership_after_cas_before_rpc(
    harness: "Callable[..., Any]", monkeypatch: "pytest.MonkeyPatch", change: "str"
) -> "None":
    live = await harness()
    storage = live.service.get_queue_backend()
    record = await storage.enqueue("cloudtasks.after_cas", execution_backend="cloudtasks", max_retries=2)

    async def client_after_change(self: "Any") -> "Any":
        assert record.execution_ref is not None
        if change == "interrupt":
            raise asyncio.CancelledError
        if change in {"cancel", "delete"}:
            await storage.cancel_task(record.id)
            if change == "delete":
                await storage.cleanup_terminal(datetime.now(timezone.utc) + timedelta(seconds=1))
        elif change in {"claim", "retry"}:
            await storage.claim_task(record.id)
            if change == "retry":
                await storage.fail_task(record.id, "retry", retry=True)
        elif change == "backend":
            await storage.set_execution_backend(record.id, "cloudrun")
        else:
            record.expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
        return live.client

    monkeypatch.setattr(type(live.backend), "_get_client", client_after_change)
    if change == "interrupt":
        with pytest.raises(asyncio.CancelledError):
            await live.repair(limit=1)
        assert record.execution_ref is not None and record.status == "pending"
    else:
        assert await live.repair(limit=1) == DispatchRepairResult(examined=1, unchanged=1, limit_reached=True)
    assert live.client.create_calls == []


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

    assert outcome == DispatchRepairResult(examined=1, unchanged=1)
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

    assert outcome == DispatchRepairResult(examined=2, changed=2, limit_reached=True)
    assert len(live.client.get_calls) == 2


async def test_a_running_record_is_never_re_delivered(harness: "Callable[..., Any]") -> "None":
    """Running means the consumer has it and Cloud Tasks is holding the response open."""
    _register("cloudtasks.repair.running")
    live = await harness()
    record = await live.enqueue("cloudtasks.repair.running")
    assert await live.service.get_queue_backend().claim_task(record.id) is not None
    live.forget_every_delivery()

    outcome = await live.repair(limit=10)

    assert outcome == DispatchRepairResult(examined=0, changed=0)
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

    async def stale_listing(self: "Any", execution_backend: "str", *, limit: "int") -> "DispatchRepairCandidates":
        return DispatchRepairCandidates(records=(snapshot,), examined=1)

    monkeypatch.setattr(type(queue_backend), "list_dispatch_repair_candidates", stale_listing)

    outcome = await live.repair(limit=10)

    assert outcome == DispatchRepairResult(examined=1, unchanged=1)
    assert live.client.get_calls == []


async def test_a_record_owned_by_another_execution_backend_is_left_alone(harness: "Callable[..., Any]") -> "None":
    live = await harness()
    alien = await live.service.get_queue_backend().enqueue("tasks.alien", execution_backend="cloudrun")
    await live.service.get_queue_backend().set_execution_ref(alien.id, "cloudrun", "jobs/alien-123")

    outcome = await live.repair(limit=10)

    assert outcome == DispatchRepairResult(examined=0, changed=0)
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

    assert outcome == DispatchRepairResult(examined=1, failed=1)
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

    assert outcome == DispatchRepairResult(examined=2, changed=1, failed=1)


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

    assert outcome == DispatchRepairResult(examined=1, failed=1)
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
    assert budgets == []


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
    assert budgets == [4]


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


@pytest.mark.parametrize("failure_at", ["lookup", "create"])
async def test_repair_failure_reaches_real_maintenance(harness: "Callable[..., Any]", failure_at: "str") -> "None":
    from litestar_queues import QueueMaintenanceConfig, QueueMaintenanceService
    from litestar_queues._cli import _maintenance_exit_code

    _register("cloudtasks.repair.maintenance_failure")
    live = await harness()
    for _ in range(2):
        if failure_at == "lookup":
            await live.enqueue("cloudtasks.repair.maintenance_failure")
        else:
            await live.service.get_queue_backend().enqueue(
                "cloudtasks.repair.maintenance_failure", execution_backend="cloudtasks"
            )

    async def fail_provider(value: "Any") -> "None":
        msg = "credentials=must-not-reach-maintenance"
        raise ServiceUnavailable(msg)

    if failure_at == "lookup":
        live.client.on_get = fail_provider
    else:
        live.client.on_create = fail_provider
    summary = await QueueMaintenanceService(live.service, QueueMaintenanceConfig(external_limit=5)).run()

    assert summary.outcome == "failed"
    assert _maintenance_exit_code(summary) == 1
    assert "must-not-reach-maintenance" not in str(summary.to_payload())


@pytest.mark.parametrize("failure_at", ["get", "reserve", "client", "request"])
async def test_repair_failure_before_create_is_counted_and_continues(
    harness: "Callable[..., Any]", monkeypatch: "pytest.MonkeyPatch", failure_at: "str"
) -> "None":
    from litestar_queues.execution.cloudtasks import backend as cloudtasks_module

    live = await harness()
    storage = live.service.get_queue_backend()
    for _ in range(2):
        await storage.enqueue("cloudtasks.repair.boundary", execution_backend="cloudtasks")

    async def refuse_async(*args: "Any", **kwargs: "Any") -> "Any":
        msg = "credential-bearing-provider-failure"
        raise RuntimeError(msg)

    def refuse_sync(*args: "Any", **kwargs: "Any") -> "Any":
        msg = "credential-bearing-provider-failure"
        raise RuntimeError(msg)

    if failure_at == "get":
        monkeypatch.setattr(type(storage), "get_task", refuse_async)
    elif failure_at == "reserve":
        monkeypatch.setattr(type(storage), "reserve_scheduled_execution_ref", refuse_async)
    elif failure_at == "client":
        monkeypatch.setattr(type(live.backend), "_get_client", refuse_async)
    else:
        monkeypatch.setattr(cloudtasks_module, "_create_task_request", refuse_sync)

    result = await live.repair(limit=5)

    assert result == DispatchRepairResult(examined=2, failed=2)
    assert len(live.repair_failures()) == 2
    assert live.client.create_calls == []


async def test_repair_failure_diagnostics_do_not_double_count(
    harness: "Callable[..., Any]", monkeypatch: "pytest.MonkeyPatch"
) -> "None":
    live = await harness()
    for _ in range(2):
        await live.service.get_queue_backend().enqueue("cloudtasks.repair.diagnostics", execution_backend="cloudtasks")

    async def refuse(*args: "Any", **kwargs: "Any") -> "Any":
        msg = "private-provider-or-sink-text"
        raise RuntimeError(msg)

    live.client.on_create = refuse
    monkeypatch.setattr(type(live.events), "publish", refuse)

    assert await live.repair(limit=5) == DispatchRepairResult(examined=2, failed=2)
    assert len(live.client.create_calls) == 2


async def test_repair_mixed_structured_and_integer_failure_results(harness: "Callable[..., Any]") -> "None":
    from litestar_queues import QueueDispatchRepairError

    _register("cloudtasks.repair.mixed_present")
    live = await harness()
    await live.enqueue("cloudtasks.repair.mixed_present")
    storage = live.service.get_queue_backend()
    await storage.enqueue("cloudtasks.repair.mixed_repaired", execution_backend="cloudtasks")
    failed = await storage.enqueue("cloudtasks.repair.mixed_failed", execution_backend="cloudtasks")

    async def fail_one(call: "CreateCall") -> "None":
        if decode_json(call.body)["task_id"] == str(failed.id):
            msg = "credential-bearing-provider-failure"
            raise ServiceUnavailable(msg)

    live.client.on_create = fail_one
    result = await live.service.reconcile_external_result(limit=5)
    assert result.repair == DispatchRepairResult(examined=3, changed=1, failed=1, unchanged=1)
    assert result.reconciled == 0
    assert result.changed == 1
    with pytest.raises(QueueDispatchRepairError, match="Queue delivery repair failed") as raised:
        await live.service.reconcile_external(limit=5)
    assert raised.value.result.repair == DispatchRepairResult(examined=3, failed=1, unchanged=2)
    assert "credential" not in str(raised.value)


async def test_repair_budget_stale_page_skips_reconciliation(
    harness: "Callable[..., Any]", monkeypatch: "pytest.MonkeyPatch"
) -> "None":
    live = await harness()

    async def stale_page(self: "Any", execution_backend: "str", *, limit: "int") -> "DispatchRepairCandidates":
        return DispatchRepairCandidates(examined=limit, limit_reached=True)

    async def forbidden(*args: "Any", **kwargs: "Any") -> "Any":
        pytest.fail("The consumed budget must not query reconciliation or providers.")

    storage = live.service.get_queue_backend()
    monkeypatch.setattr(type(storage), "list_dispatch_repair_candidates", stale_page)
    monkeypatch.setattr(type(storage), "list_running_external", forbidden)
    live.client.on_get = forbidden
    live.client.on_create = forbidden
    result = await live.service.reconcile_external_result(limit=3)
    assert result.repair == DispatchRepairResult(examined=3, unchanged=3, limit_reached=True)
    assert result.changed == 0


async def test_repair_budget_zero_and_negative_do_not_access_backends(
    harness: "Callable[..., Any]", monkeypatch: "pytest.MonkeyPatch"
) -> "None":
    live = await harness()

    def forbidden(*args: "Any", **kwargs: "Any") -> "Any":
        pytest.fail("A nonpositive budget must not access storage or execution backends.")

    monkeypatch.setattr(QueueService, "get_queue_backend", forbidden)
    monkeypatch.setattr(QueueService, "get_execution_backend", forbidden)
    result = await live.service.reconcile_external_result(limit=0)
    assert result.repair == DispatchRepairResult(limit_reached=True)
    assert result.changed == 0
    assert await live.service.reconcile_external(limit=0) == 0
    with pytest.raises(QueueConfigurationError, match="non-negative"):
        await live.service.reconcile_external_result(limit=-1)


async def test_repair_budget_preserves_legacy_result_defaults(
    harness: "Callable[..., Any]", monkeypatch: "pytest.MonkeyPatch"
) -> "None":
    live = await harness()
    budgets: "list[int | None]" = []

    async def legacy_repair(self: "Any", service: "Any", *, limit: "int") -> "DispatchRepairResult":
        return DispatchRepairResult(examined=2, changed=1)

    async def reconcile(self: "Any", *, limit: "int | None") -> "int":
        budgets.append(limit)
        return 2

    monkeypatch.setattr(type(live.backend), "repair", legacy_repair)
    monkeypatch.setattr(QueueService, "_reconcile_external_records", reconcile)
    result = await live.service.reconcile_external_result(limit=5)
    assert result.repair == DispatchRepairResult(examined=2, changed=1)
    assert result.reconciled == 2
    assert result.changed == 3
    assert budgets == [3]


async def test_repair_failure_in_success_diagnostics_counts_candidate_once(
    harness: "Callable[..., Any]", monkeypatch: "pytest.MonkeyPatch"
) -> "None":
    from litestar_queues.execution.cloudtasks import backend as cloudtasks_module

    live = await harness()
    storage = live.service.get_queue_backend()
    first = await storage.enqueue("cloudtasks.repair.metric_failure", execution_backend="cloudtasks")
    await storage.enqueue("cloudtasks.repair.metric_success", execution_backend="cloudtasks")
    original = cloudtasks_module._record_outcome

    def fail_first_success(service: "Any", record: "Any", operation: "Any", outcome: "str") -> "None":
        if record.id == first.id and outcome == operation.created:
            msg = "diagnostic failure after provider creation"
            raise RuntimeError(msg)
        original(service, record, operation, outcome)

    monkeypatch.setattr(cloudtasks_module, "_record_outcome", fail_first_success)
    result = await live.repair(limit=5)
    assert result == DispatchRepairResult(examined=2, changed=1, failed=1)
    assert len(live.client.create_calls) == 2
    assert len(live.repair_failures()) == 1


async def test_repair_failure_cancellation_propagates_and_releases_maintenance(harness: "Callable[..., Any]") -> "None":
    from litestar_queues import QueueMaintenanceConfig, QueueMaintenanceService

    live = await harness()
    await live.service.get_queue_backend().enqueue("cloudtasks.repair.cancel", execution_backend="cloudtasks")

    async def cancelled(call: "CreateCall") -> "None":
        raise asyncio.CancelledError

    live.client.on_create = cancelled
    maintenance = QueueMaintenanceService(live.service, QueueMaintenanceConfig(external_limit=5))
    with pytest.raises(asyncio.CancelledError):
        await maintenance.run()
    assert live.repair_failures() == []
    live.client.on_create = None
    recovered = await maintenance.run()
    assert recovered.acquired is True
    assert recovered.outcome == "completed"
    assert recovered.phases[0].changed == 1

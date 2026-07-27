"""Per-record limits a Cloud Tasks queue enforces before anything is persisted.

Cloud Tasks accepts a task and then owns its delivery, so a record whose timeout
outlasts the HTTP budget, or whose schedule is further out than Google will
hold, can only fail after the fact -- repeatedly. Each rule therefore runs
before reservation and before the record is written.
"""

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

import pytest

from litestar_queues import QueueConfig, QueueService, WorkerConfig, task
from litestar_queues.exceptions import QueueConfigurationError

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable

    from litestar_queues.execution.cloudtasks import CloudTasksExecutionConfig

pytestmark = pytest.mark.anyio

_FIXED_NOW = datetime(2026, 7, 27, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def frozen_now(monkeypatch: "pytest.MonkeyPatch") -> "datetime":
    """Pin the clock the horizon check reads so boundary cases cannot race.

    Returns:
        The fixed UTC instant every enqueue in this module compares against.
    """
    from litestar_queues import service as service_module

    monkeypatch.setattr(service_module, "_utc_now", lambda: _FIXED_NOW)
    return _FIXED_NOW


@pytest.fixture
async def cloud_tasks_service(
    shared_storage: "str", cloud_tasks_config: "Callable[..., CloudTasksExecutionConfig]"
) -> "AsyncIterator[QueueService]":
    """Yield a lifecycle-managed service on a Cloud Tasks queue.

    An accepted record is scheduled for delivery on the way out of ``enqueue``,
    so the client is injected: these tests are about what the queue refuses to
    persist, not about what it sends.

    Yields:
        A service whose execution backend is a typed Cloud Tasks config.
    """
    from litestar_queues.execution.cloudtasks import CloudTasksExecutionBackend
    from tests.unit.execution.cloudtasks._fakes import FakeCloudTasksClient

    execution_config = cloud_tasks_config()
    async with QueueService(
        QueueConfig(
            queue_backend=shared_storage, execution_backend=execution_config, worker=WorkerConfig(placement="external")
        ),
        execution_backend=CloudTasksExecutionBackend(execution_config=execution_config, client=FakeCloudTasksClient()),
    ) as service:
        yield service


async def _record_metadata(service: "QueueService", task_id: "Any") -> "dict[str, Any]":
    record = await service.get_queue_backend().get_task(task_id)
    assert record is not None
    return dict(record.metadata)


# --------------------------------------------------------------------------- timeout


async def test_the_configured_default_timeout_is_written_onto_the_record(cloud_tasks_service: "QueueService") -> "None":
    """The consumer must not have to re-derive the budget the producer accepted."""

    @task("cloudtasks.limits.default_timeout")
    async def probe() -> "None":
        return None

    result = await cloud_tasks_service.enqueue(probe)

    assert (await _record_metadata(cloud_tasks_service, result.id))["timeout"] == 1740.0


async def test_an_explicit_timeout_inside_the_budget_is_accepted(cloud_tasks_service: "QueueService") -> "None":
    @task("cloudtasks.limits.explicit_timeout")
    async def probe() -> "None":
        return None

    result = await cloud_tasks_service.enqueue(probe, timeout=60.0)

    assert (await _record_metadata(cloud_tasks_service, result.id))["timeout"] == 60.0


async def test_a_decorator_timeout_inside_the_budget_is_accepted(cloud_tasks_service: "QueueService") -> "None":
    @task("cloudtasks.limits.decorated_timeout", timeout=120)
    async def probe() -> "None":
        return None

    result = await cloud_tasks_service.enqueue(probe)

    assert (await _record_metadata(cloud_tasks_service, result.id))["timeout"] == 120


@pytest.mark.parametrize("timeout", [True, 0, -1.0, float("nan"), float("inf")])
async def test_a_timeout_that_is_not_a_finite_positive_number_is_rejected(
    cloud_tasks_service: "QueueService", timeout: "Any"
) -> "None":
    """``bool`` is an ``int`` subclass, so a truthy flag would pass as one second."""

    @task("cloudtasks.limits.bad_timeout")
    async def probe() -> "None":
        return None

    with pytest.raises(QueueConfigurationError):
        await cloud_tasks_service.enqueue(probe, timeout=timeout)


async def test_a_timeout_that_outlasts_the_delivery_budget_is_rejected(cloud_tasks_service: "QueueService") -> "None":
    """1780 + 30 exceeds the 1800s deadline, so the response could never arrive."""

    @task("cloudtasks.limits.overlong_timeout")
    async def probe() -> "None":
        return None

    with pytest.raises(QueueConfigurationError):
        await cloud_tasks_service.enqueue(probe, timeout=1780.0)


async def test_a_decorator_timeout_that_outlasts_the_delivery_budget_is_rejected(
    cloud_tasks_service: "QueueService",
) -> "None":
    @task("cloudtasks.limits.overlong_decorated", timeout=1780)
    async def probe() -> "None":
        return None

    with pytest.raises(QueueConfigurationError):
        await cloud_tasks_service.enqueue(probe)


async def test_a_smaller_deadline_shrinks_what_the_queue_accepts(
    shared_storage: "str", cloud_tasks_config: "Callable[..., CloudTasksExecutionConfig]"
) -> "None":
    """The budget is the configured deadline, not the 1800s ceiling."""

    @task("cloudtasks.limits.small_deadline")
    async def probe() -> "None":
        return None

    async with QueueService(
        QueueConfig(
            queue_backend=shared_storage,
            execution_backend=cloud_tasks_config(dispatch_deadline=120, default_task_timeout=60.0),
            worker=WorkerConfig(placement="external"),
        )
    ) as service:
        with pytest.raises(QueueConfigurationError):
            await service.enqueue(probe, timeout=100.0)


# --------------------------------------------------------------------------- schedule horizon


@pytest.mark.usefixtures("frozen_now")
async def test_scheduling_exactly_thirty_days_ahead_is_accepted(cloud_tasks_service: "QueueService") -> "None":
    @task("cloudtasks.limits.horizon_edge")
    async def probe() -> "None":
        return None

    result = await cloud_tasks_service.enqueue(probe, scheduled_at=_FIXED_NOW + timedelta(days=30))

    assert result.id is not None


@pytest.mark.usefixtures("frozen_now")
async def test_scheduling_beyond_thirty_days_is_rejected(cloud_tasks_service: "QueueService") -> "None":
    """Cloud Tasks refuses the create call, so accepting it here only defers the error."""

    @task("cloudtasks.limits.horizon_over")
    async def probe() -> "None":
        return None

    with pytest.raises(QueueConfigurationError):
        await cloud_tasks_service.enqueue(probe, scheduled_at=_FIXED_NOW + timedelta(days=30, seconds=1))


@pytest.mark.usefixtures("frozen_now")
async def test_a_naive_scheduled_at_is_normalized_before_the_horizon_check(
    cloud_tasks_service: "QueueService",
) -> "None":
    """Comparing a naive datetime against an aware one raises TypeError, not a config error."""

    @task("cloudtasks.limits.horizon_naive")
    async def probe() -> "None":
        return None

    naive = (_FIXED_NOW + timedelta(days=30, seconds=1)).replace(tzinfo=None)

    with pytest.raises(QueueConfigurationError):
        await cloud_tasks_service.enqueue(probe, scheduled_at=naive)


# --------------------------------------------------------------------------- fail before persistence


async def test_a_rejected_enqueue_writes_no_record_and_no_reservation(cloud_tasks_service: "QueueService") -> "None":
    """Rejection has to precede reservation, or a bad call burns the identity."""

    @task("cloudtasks.limits.no_write", key="limits-no-write", unique_until="forever")
    async def probe() -> "None":
        return None

    with pytest.raises(QueueConfigurationError):
        await cloud_tasks_service.enqueue(probe, timeout=1780.0)

    assert await cloud_tasks_service.get_task_identity("limits-no-write") is None
    assert await cloud_tasks_service.get_queue_backend().list_pending(limit=10) == []


# --------------------------------------------------------------------------- single-backend rule


async def test_the_horizon_and_budget_do_not_apply_to_a_polled_queue() -> "None":
    """These limits belong to the transport, not to every queue in the package."""

    @task("cloudtasks.limits.polled")
    async def probe() -> "None":
        return None

    async with QueueService(
        QueueConfig(queue_backend="memory", execution_backend="local", worker=WorkerConfig(placement="external"))
    ) as service:
        result = await service.enqueue(
            probe, timeout=100000.0, scheduled_at=datetime.now(timezone.utc) + timedelta(days=365)
        )

    assert result.id is not None

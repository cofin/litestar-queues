"""The self-dispatching seam and the single-backend rule it depends on.

A Cloud Tasks queue has no resident worker: the record is scheduled for delivery
the moment it is durably persisted. That inverts two assumptions the package
holds elsewhere -- that a pending record is claimed by a polling worker, and
that ``execution_backend`` may vary per record -- so both are made explicit here
rather than discovered when a record silently never runs.
"""

from typing import TYPE_CHECKING, Any

import pytest

from litestar_queues import QueueConfig, QueueService, WorkerConfig, task
from litestar_queues.exceptions import QueueConfigurationError
from litestar_queues.execution import BaseExecutionBackend

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from litestar_queues.execution.cloudtasks import CloudTasksExecutionConfig
    from litestar_queues.models import QueuedTaskRecord

pytestmark = pytest.mark.anyio

_SELF_SCHEDULING = "selftest-self-scheduling"

scheduled: "list[Any]" = []


class _SelfSchedulingBackend(BaseExecutionBackend):
    """Stands in for any backend that schedules its own delivery at enqueue time."""

    @property
    def is_external(self) -> "bool":
        return True

    @property
    def schedules_on_enqueue(self) -> "bool":
        return True

    async def schedule(self, service: "QueueService", record: "QueuedTaskRecord") -> "str | None":
        del service
        scheduled.append(record.id)
        return "scheduled-reference"


@pytest.fixture(autouse=True)
def _reset_scheduled() -> "Iterator[None]":
    scheduled.clear()
    yield
    scheduled.clear()


@pytest.fixture
def self_scheduling_execution() -> "Iterator[str]":
    from litestar_queues.execution.factory import _execution_backend_registry

    _execution_backend_registry[_SELF_SCHEDULING] = _SelfSchedulingBackend
    yield _SELF_SCHEDULING
    _execution_backend_registry.pop(_SELF_SCHEDULING, None)


# --------------------------------------------------------------------------- capability seam


def test_the_base_backend_does_not_schedule_on_enqueue() -> "None":
    """Opt-in by default: an existing backend keeps its resident-worker contract."""
    assert BaseExecutionBackend().schedules_on_enqueue is False


def test_external_dispatch_and_self_scheduling_are_separate_capabilities() -> "None":
    """Cloud Run Jobs is external but still needs a worker to notice a pending record.

    Collapsing the two flags would make every future broker backend self-scheduling
    and silently retire the worker loop they rely on.
    """
    from litestar_queues.execution import ImmediateExecutionBackend, LocalExecutionBackend
    from litestar_queues.execution.cloudrun import CloudRunExecutionBackend

    cloudrun = CloudRunExecutionBackend()

    assert cloudrun.is_external is True
    assert cloudrun.schedules_on_enqueue is False
    assert LocalExecutionBackend().schedules_on_enqueue is False
    assert ImmediateExecutionBackend().schedules_on_enqueue is False


def test_the_cloud_tasks_backend_declares_both_capabilities() -> "None":
    from litestar_queues.execution.cloudtasks import CloudTasksExecutionBackend

    backend = CloudTasksExecutionBackend()

    assert backend.is_external is True
    assert backend.schedules_on_enqueue is True


async def test_the_default_schedule_implementation_delegates_to_dispatch() -> "None":
    """A backend that self-schedules over its normal dispatch path writes no new code."""
    dispatched: "list[str]" = []

    class _DispatchOnly(BaseExecutionBackend):
        async def dispatch(self, service: "QueueService", record: "QueuedTaskRecord") -> "str | None":
            dispatched.append(record.task_name)
            return "dispatch-reference"

    from litestar_queues.models import QueuedTaskRecord

    record = QueuedTaskRecord(task_name="example")
    service = QueueService(QueueConfig(queue_backend="memory", worker=WorkerConfig(placement="external")))

    assert await _DispatchOnly().schedule(service, record) == "dispatch-reference"
    assert dispatched == ["example"]


# --------------------------------------------------------------------------- service boundary


async def test_schedule_persisted_is_a_no_op_for_a_polled_backend() -> "None":
    """Every persistence path may call the seam; only self-scheduling backends react."""
    from litestar_queues.models import QueuedTaskRecord

    record = QueuedTaskRecord(task_name="example")
    async with QueueService(
        QueueConfig(queue_backend="memory", execution_backend="local", worker=WorkerConfig(placement="external"))
    ) as service:
        assert await service._schedule_persisted(record) is record

    assert scheduled == []


async def test_schedule_persisted_reloads_the_record_after_scheduling(
    shared_storage: "str", self_scheduling_execution: "str"
) -> "None":
    """The backend may write a delivery reference, so the caller must not keep a stale copy."""

    @task("cloudtasks.seam_probe")
    async def probe() -> "None":
        return None

    async with QueueService(
        QueueConfig(
            queue_backend=shared_storage,
            execution_backend=self_scheduling_execution,
            worker=WorkerConfig(placement="external"),
        )
    ) as service:
        result = await service.enqueue(probe)
        record = await service.get_queue_backend().get_task(result.id)
        assert record is not None
        scheduled.clear()

        returned = await service._schedule_persisted(record)

    assert scheduled == [record.id]
    assert returned.id == record.id


# --------------------------------------------------------------------------- worker refusal


async def test_run_once_refuses_a_self_dispatching_backend(
    shared_storage: "str", self_scheduling_execution: "str"
) -> "None":
    """Returning zero would look like an idle queue while records are already in flight."""
    from litestar_queues import Worker

    async with QueueService(
        QueueConfig(
            queue_backend=shared_storage,
            execution_backend=self_scheduling_execution,
            worker=WorkerConfig(placement="external"),
        )
    ) as service:
        with pytest.raises(QueueConfigurationError):
            await Worker(service).run_once()


def test_the_standalone_worker_command_rejects_a_self_dispatching_backend(
    cloud_tasks_config: "Callable[..., CloudTasksExecutionConfig]",
) -> "None":
    """``litestar queues run`` is the user-facing guard, so it fails with a clear message."""
    import click

    from litestar_queues import QueuePlugin
    from litestar_queues._cli import _reject_self_dispatching_execution

    plugin = QueuePlugin(
        QueueConfig(
            queue_backend="redis", execution_backend=cloud_tasks_config(), worker=WorkerConfig(placement="external")
        )
    )

    with pytest.raises(click.ClickException):
        _reject_self_dispatching_execution(plugin)


def test_the_standalone_worker_command_accepts_a_polled_backend() -> "None":
    from litestar_queues import QueuePlugin
    from litestar_queues._cli import _reject_self_dispatching_execution

    plugin = QueuePlugin(
        QueueConfig(queue_backend="redis", execution_backend="local", worker=WorkerConfig(placement="external"))
    )

    _reject_self_dispatching_execution(plugin)


# --------------------------------------------------------------------------- single-backend rule


async def test_a_cloud_tasks_queue_rejects_a_different_execution_override(
    shared_storage: "str", cloud_tasks_config: "Callable[..., CloudTasksExecutionConfig]"
) -> "None":
    """There is no worker for a ``local`` record on a Cloud Tasks queue.

    Rejecting at enqueue is the only place this is visible; accepting it writes a
    record nothing will ever claim.
    """

    @task("cloudtasks.override_probe")
    async def probe() -> "None":
        return None

    async with QueueService(
        QueueConfig(
            queue_backend=shared_storage,
            execution_backend=cloud_tasks_config(),
            worker=WorkerConfig(placement="external"),
        )
    ) as service:
        with pytest.raises(QueueConfigurationError):
            await service.enqueue(probe, execution_backend="local")


async def test_a_cloud_tasks_queue_rejects_a_decorator_execution_override(
    shared_storage: "str", cloud_tasks_config: "Callable[..., CloudTasksExecutionConfig]"
) -> "None":
    @task("cloudtasks.decorated_probe", execution_backend="cloudrun")
    async def probe() -> "None":
        return None

    async with QueueService(
        QueueConfig(
            queue_backend=shared_storage,
            execution_backend=cloud_tasks_config(),
            worker=WorkerConfig(placement="external"),
        )
    ) as service:
        with pytest.raises(QueueConfigurationError):
            await service.enqueue(probe)


async def test_a_polled_queue_rejects_a_bare_cloudtasks_override() -> "None":
    """No typed config means no project, queue, target, or audience to deliver with."""

    @task("cloudtasks.bare_probe")
    async def probe() -> "None":
        return None

    async with QueueService(
        QueueConfig(queue_backend="memory", execution_backend="local", worker=WorkerConfig(placement="external"))
    ) as service:
        with pytest.raises(QueueConfigurationError):
            await service.enqueue(probe, execution_backend="cloudtasks")

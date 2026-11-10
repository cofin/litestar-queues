from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import pytest

from litestar_queues import HeartbeatTouch, InMemoryQueueBackend
from litestar_queues.backends import BaseQueueBackend
from litestar_queues.backends.base import (
    VALID_TASK_STATUSES,
    attempts_consumed,
    interruption_count,
    normalize_task_list_query,
    normalize_task_statuses,
    record_matches_filters,
    retry_schedule,
    stale_requeue_priority,
    validate_task_list_pagination,
)
from litestar_queues.exceptions import QueueConfigurationError
from litestar_queues.models import QueuedTaskRecord, TaskStatus

pytestmark = pytest.mark.anyio


class _MaintenanceOnlyBackend(BaseQueueBackend):
    """A backend that implements only token-fenced maintenance coordination."""

    __slots__ = ("acquire_calls", "held")

    def __init__(self) -> "None":
        super().__init__()
        self.held: "dict[str, str]" = {}
        self.acquire_calls: "list[tuple[str, str]]" = []

    async def acquire_maintenance(self, name: "str", token: "str", *, ttl: "timedelta") -> "bool":
        del ttl
        self.acquire_calls.append((name, token))
        if name in self.held and self.held[name] != token:
            return False
        self.held[name] = token
        return True


def test_base_backend_has_no_single_task_heartbeat_api() -> "None":
    single_task_heartbeat_api = "touch" + "_heartbeat"

    assert not hasattr(BaseQueueBackend, single_task_heartbeat_api)


async def test_worker_lock_denies_a_second_holder_while_the_first_is_live() -> "None":
    backend = _MaintenanceOnlyBackend()

    first = await backend.acquire_worker_lock("stale_recovery", ttl=timedelta(seconds=60))
    second = await backend.acquire_worker_lock("stale_recovery", ttl=timedelta(seconds=60))

    assert first is True
    assert second is False


async def test_worker_lock_uses_a_distinct_token_per_acquisition() -> "None":
    backend = _MaintenanceOnlyBackend()

    await backend.acquire_worker_lock("expiry_sweep", ttl=timedelta(seconds=60))
    await backend.acquire_worker_lock("expiry_sweep", ttl=timedelta(seconds=60))

    tokens = [token for _, token in backend.acquire_calls]
    assert len(set(tokens)) == 2


async def test_worker_control_wait_defaults_to_sleep_and_report_nothing() -> "None":
    """Polling-only backends inherit a wait that never claims a hint arrived."""
    backend = _MaintenanceOnlyBackend()

    assert await backend.wait_for_worker_control(worker_id="worker-a", timeout=0) is False
    assert await backend.wait_for_worker_control(worker_id="worker-a") is False


_REQUIRED_CONTRACT_CALLS = {
    "cleanup_terminal": lambda b: b.cleanup_terminal(datetime.now(UTC)),
    "expire_overdue": lambda b: b.expire_overdue(),
    "get_statistics": lambda b: b.get_statistics(),
    "list_completed_by_task": lambda b: b.list_completed_by_task("tasks.sync"),
    "list_running_external": lambda b: b.list_running_external(),
    "list_tasks": lambda b: b.list_tasks(),
    "assign_worker": lambda b: b.assign_worker(uuid4(), worker_id="worker-a", expected_retry_count=0),
    "interrupt_task": lambda b: b.interrupt_task(
        uuid4(), expected_retry_count=0, worker_id="worker-a", queued_at=datetime.now(UTC)
    ),
    "null_heartbeats": lambda b: b.null_heartbeats([uuid4()]),
    "requeue_stale_running": lambda b: b.requeue_stale_running(stale_after=timedelta(seconds=60)),
    "set_execution_backend": lambda b: b.set_execution_backend(uuid4(), "cloudrun"),
    "set_execution_ref": lambda b: b.set_execution_ref(uuid4(), "cloudrun", "jobs/1"),
    "touch_heartbeats": lambda b: b.touch_heartbeats([HeartbeatTouch(task_id=uuid4(), expected_retry_count=None)]),
}


@pytest.mark.parametrize("method", sorted(_REQUIRED_CONTRACT_CALLS))
async def test_required_backend_contract_methods_refuse_to_no_op(method: "str") -> "None":
    """A backend that cannot answer these must fail loudly, not silently succeed.

    Each default previously returned an empty or unpersisted result, so a
    backend that forgot to implement one would silently skip maintenance or
    drop an execution reference instead of reporting the gap.
    """
    with pytest.raises(NotImplementedError):
        await _REQUIRED_CONTRACT_CALLS[method](BaseQueueBackend())


async def test_worker_lock_is_fenced_on_a_backend_with_real_maintenance() -> "None":
    backend = InMemoryQueueBackend()

    first = await backend.acquire_worker_lock("external_reconcile", ttl=timedelta(seconds=60))
    second = await backend.acquire_worker_lock("external_reconcile", ttl=timedelta(seconds=60))

    assert first is True
    assert second is False


def test_interruption_count_reads_only_positive_integers() -> "None":
    assert interruption_count(QueuedTaskRecord("tasks.a")) == 0
    assert interruption_count(QueuedTaskRecord("tasks.a", metadata={"interruptions": None})) == 0
    assert interruption_count(QueuedTaskRecord("tasks.a", metadata={"interruptions": "two"})) == 0
    assert interruption_count(QueuedTaskRecord("tasks.a", metadata={"interruptions": -1})) == 0
    assert interruption_count(QueuedTaskRecord("tasks.a", metadata={"interruptions": 2})) == 2


def test_attempts_consumed_discounts_interruptions() -> "None":
    record = QueuedTaskRecord("tasks.a", retry_count=3, metadata={"interruptions": 2})

    assert attempts_consumed(record) == 1
    assert attempts_consumed(QueuedTaskRecord("tasks.a", retry_count=3)) == 3


def test_retry_schedule_backoff_uses_attempts_consumed() -> "None":
    backoff = {"initial_delay": 1.0, "multiplier": 2.0}
    interrupted = QueuedTaskRecord("tasks.a", retry_count=3, metadata={"interruptions": 2, "retry_backoff": backoff})
    plain = QueuedTaskRecord("tasks.a", retry_count=1, metadata={"retry_backoff": backoff})

    queued_at, retry_at = retry_schedule(interrupted)
    plain_queued_at, plain_retry_at = retry_schedule(plain)

    assert retry_at is not None
    assert plain_retry_at is not None
    assert (retry_at - queued_at) == (plain_retry_at - plain_queued_at)


def test_stale_requeue_priority_policies() -> "None":
    assert stale_requeue_priority(9, 4) == 4
    assert stale_requeue_priority(2, 4) == 2
    assert stale_requeue_priority(9, "preserve") == 9
    assert stale_requeue_priority(9, lambda priority: priority - 1) == 8


def test_stale_requeue_priority_rejects_a_non_integer_callable() -> "None":
    with pytest.raises(QueueConfigurationError):
        stale_requeue_priority(9, lambda _priority: "high")  # type: ignore[arg-type,return-value]


def test_valid_task_statuses_matches_task_status_literal() -> "None":
    assert frozenset({"pending", "scheduled", "running", "completed", "failed", "cancelled", "expired"}) == (
        VALID_TASK_STATUSES
    )


def test_task_list_validation_helpers_direct_defaults() -> "None":
    assert validate_task_list_pagination() == (50, 0)
    assert normalize_task_statuses("pending") == ("pending",)
    assert normalize_task_statuses(None) is None


@pytest.mark.parametrize(
    ("status", "limit", "offset", "expected"),
    [
        (None, 50, 0, (None, 50, 0)),
        ("failed", 1, 2, (("failed",), 1, 2)),
        (["completed", "failed", "completed"], 50, 0, (("completed", "failed"), 50, 0)),
        ((), 50, 0, ((), 50, 0)),
        ([], 10, 5, ((), 10, 5)),
    ],
)
def test_normalize_task_list_query_valid_inputs(
    status: "Any", limit: "int", offset: "int", expected: "tuple[tuple[TaskStatus, ...] | None, int, int]"
) -> "None":
    assert normalize_task_list_query(status=status, limit=limit, offset=offset) == expected


@pytest.mark.parametrize(
    ("status", "limit", "offset"),
    [
        (None, 0, 0),
        (None, -5, 0),
        (None, True, 0),
        (None, False, 0),
        (None, 1.5, 0),
        (None, "10", 0),
        (None, None, 0),
        (None, 50, -1),
        (None, 50, False),
        (None, 50, True),
        (None, 50, 1.5),
        (None, 50, "1"),
        (None, 50, None),
        ((), 0, 0),
        ((), 50, -1),
    ],
)
def test_normalize_task_list_query_rejects_invalid_pagination(status: "Any", limit: "Any", offset: "Any") -> "None":
    with pytest.raises(QueueConfigurationError):
        normalize_task_list_query(status=status, limit=limit, offset=offset)


@pytest.mark.parametrize(
    "status",
    ["bogus", "", ["pending", "invalid"], b"pending", b"", {"pending"}, 123, True, {"pending": 1}, ["pending", 123]],
)
def test_normalize_task_list_query_rejects_invalid_statuses(status: "Any") -> "None":
    with pytest.raises(QueueConfigurationError):
        normalize_task_list_query(status=status)


@pytest.mark.parametrize(
    ("record", "statuses", "key", "expected"),
    [
        (QueuedTaskRecord(task_name="tasks.a", status="failed", key="k-1"), {"completed", "failed"}, "k-1", True),
        (QueuedTaskRecord(task_name="tasks.a", status="failed", key="k-1"), {"completed", "failed"}, "k-2", False),
        (QueuedTaskRecord(task_name="tasks.a", status="failed", key="k-1"), (), "k-1", False),
        (QueuedTaskRecord(task_name="tasks.a", status="failed", key="k-1"), None, "k-1", True),
        (QueuedTaskRecord(task_name="tasks.a", status="failed", key="k-1"), {"failed"}, None, True),
        (QueuedTaskRecord(task_name="tasks.a", status="failed", key="k-1"), {"pending"}, "k-1", False),
        (QueuedTaskRecord(task_name="tasks.a", status="pending", key=None), {"pending"}, "k-1", False),
    ],
)
def test_record_matches_filters_status_and_key(
    record: "QueuedTaskRecord",
    statuses: "set[TaskStatus] | tuple[TaskStatus, ...] | None",
    key: "str | None",
    expected: "bool",
) -> "None":
    assert record_matches_filters(record, statuses=statuses, key=key) is expected

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from litestar_queues import HeartbeatTouch, InMemoryQueueBackend
from litestar_queues.backends import BaseQueueBackend

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


_REQUIRED_CONTRACT_CALLS = {
    "cleanup_terminal": lambda b: b.cleanup_terminal(datetime.now(timezone.utc)),
    "expire_overdue": lambda b: b.expire_overdue(),
    "get_statistics": lambda b: b.get_statistics(),
    "list_completed_by_task": lambda b: b.list_completed_by_task("tasks.sync"),
    "list_running_external": lambda b: b.list_running_external(),
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

"""Redis distributed maintenance coordination and bounded-operation contract."""

import uuid
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, cast
from uuid import UUID

import pytest

pytest.importorskip("redis")

from litestar_queues.exceptions import QueueConfigurationError
from tests.integration.backends._maintenance_asserts import (
    assert_bounded_cleanup_terminal,
    assert_bounded_stale_recovery,
    assert_coordination_expiry,
    assert_cross_instance_coordination,
)

if TYPE_CHECKING:
    from litestar_queues.backends.redis import RedisQueueBackend
    from tests.integration.backends.redis.conftest import RedisService

pytestmark = pytest.mark.anyio


async def test_redis_backend_bounded_cleanup_terminal(redis_backend: "RedisQueueBackend") -> "None":
    await assert_bounded_cleanup_terminal(redis_backend)


async def test_redis_backend_bounded_stale_recovery(redis_backend: "RedisQueueBackend") -> "None":
    await assert_bounded_stale_recovery(redis_backend)


async def test_redis_bounded_maintenance_does_not_enumerate_status_sets(
    redis_backend: "RedisQueueBackend", monkeypatch: "pytest.MonkeyPatch"
) -> "None":
    """Positive maintenance limits must use server-bounded ordered indexes."""
    external = await redis_backend.enqueue("tasks.maintenance.external", execution_backend="cloudrun")
    claimed_external = await redis_backend.claim_task(external.id)
    assert claimed_external is not None
    await redis_backend.set_execution_ref(external.id, "cloudrun", "jobs/external")

    stale = await redis_backend.enqueue("tasks.maintenance.stale", max_retries=1)
    assert await redis_backend.claim_task(stale.id) is not None

    terminal = await redis_backend.enqueue("tasks.maintenance.terminal")
    assert await redis_backend.claim_task(terminal.id) is not None
    assert await redis_backend.complete_task(terminal.id) is not None

    async def fail_full_status_scan(*_args: "Any", **_kwargs: "Any") -> "list[Any]":
        msg = "bounded maintenance enumerated a complete status set"
        raise AssertionError(msg)

    monkeypatch.setattr(type(redis_backend), "_list_records_by_statuses", fail_full_status_scan)

    assert [record.id for record in await redis_backend.list_running_external(limit=1)] == [external.id]
    stale_result = await redis_backend.requeue_stale_running(stale_after=timedelta(seconds=-2), limit=1)
    assert stale_result.requeued + stale_result.failed == 1
    assert await redis_backend.cleanup_terminal(datetime.now(timezone.utc) + timedelta(seconds=1), limit=1) == 1


async def test_redis_maintenance_indexes_follow_lifecycle_transitions(redis_backend: "RedisQueueBackend") -> "None":
    """Running, external, and terminal indexes must not retain transitioned IDs."""
    record = await redis_backend.enqueue("tasks.maintenance.indexes", execution_backend="cloudrun", max_retries=1)
    claimed = await redis_backend.claim_task(record.id)
    assert claimed is not None
    await redis_backend.set_execution_ref(record.id, "cloudrun", "jobs/indexed")

    client = cast("Any", await redis_backend._get_client())
    task_id = str(record.id)
    assert task_id in {str(value) for value in await client.zrange(redis_backend._maintenance_running_key, 0, -1)}
    assert task_id in {str(value) for value in await client.zrange(redis_backend._maintenance_external_key, 0, -1)}

    completed = await redis_backend.complete_task(record.id)
    assert completed is not None
    assert task_id not in {str(value) for value in await client.zrange(redis_backend._maintenance_running_key, 0, -1)}
    assert task_id not in {str(value) for value in await client.zrange(redis_backend._maintenance_external_key, 0, -1)}
    assert task_id in {str(value) for value in await client.zrange(redis_backend._maintenance_terminal_key, 0, -1)}

    assert await redis_backend.cleanup_terminal(datetime.now(timezone.utc) + timedelta(seconds=1), limit=1) == 1
    assert task_id not in {str(value) for value in await client.zrange(redis_backend._maintenance_terminal_key, 0, -1)}


async def test_redis_maintenance_indexes_tie_break_equal_timestamps_by_id(
    redis_backend: "RedisQueueBackend", monkeypatch: "pytest.MonkeyPatch"
) -> "None":
    """Every bounded index uses the record ID as its deterministic tie-breaker."""
    from litestar_queues.backends.redis import backend as redis_backend_module

    fixed_now = datetime.now(timezone.utc)
    monkeypatch.setattr(redis_backend_module, "_utc_now", lambda: fixed_now)

    stale_high = await redis_backend.enqueue("tasks.maintenance.stale.high", id=UUID(int=2), max_retries=1)
    stale_low = await redis_backend.enqueue("tasks.maintenance.stale.low", id=UUID(int=1), max_retries=1)
    assert await redis_backend.claim_task(stale_high.id) is not None
    assert await redis_backend.claim_task(stale_low.id) is not None

    external_high = await redis_backend.enqueue("tasks.maintenance.external.high", id=UUID(int=4))
    external_low = await redis_backend.enqueue("tasks.maintenance.external.low", id=UUID(int=3))
    assert await redis_backend.claim_task(external_high.id) is not None
    assert await redis_backend.claim_task(external_low.id) is not None
    await redis_backend.set_execution_ref(external_high.id, "cloudrun", "jobs/high")
    await redis_backend.set_execution_ref(external_low.id, "cloudrun", "jobs/low")

    terminal_high = await redis_backend.enqueue("tasks.maintenance.terminal.high", id=UUID(int=6))
    terminal_low = await redis_backend.enqueue("tasks.maintenance.terminal.low", id=UUID(int=5))
    assert await redis_backend.claim_task(terminal_high.id) is not None
    assert await redis_backend.claim_task(terminal_low.id) is not None
    assert await redis_backend.complete_task(terminal_high.id) is not None
    assert await redis_backend.complete_task(terminal_low.id) is not None

    assert [record.id for record in await redis_backend.list_running_external(limit=1)] == [external_low.id]
    stale_result = await redis_backend.requeue_stale_running(stale_after=timedelta(seconds=-1), limit=1)
    assert stale_result.requeued == 1
    stored_stale_low = await redis_backend.get_task(stale_low.id)
    stored_stale_high = await redis_backend.get_task(stale_high.id)
    assert stored_stale_low is not None
    assert stored_stale_high is not None
    assert stored_stale_low.status == "pending"
    assert stored_stale_high.status == "running"
    assert await redis_backend.cleanup_terminal(fixed_now + timedelta(seconds=1), limit=1) == 1
    assert await redis_backend.get_task(terminal_low.id) is None
    assert await redis_backend.get_task(terminal_high.id) is not None


async def test_redis_bounded_maintenance_fails_closed_until_legacy_indexes_are_rebuilt(
    redis_backend: "RedisQueueBackend",
) -> "None":
    """A pre-index namespace must require an explicit one-time rebuild."""
    stale = await redis_backend.enqueue("tasks.maintenance.legacy.stale", max_retries=1)
    assert await redis_backend.claim_task(stale.id) is not None
    external = await redis_backend.enqueue(
        "tasks.maintenance.legacy.external", execution_backend="cloudrun", max_retries=1
    )
    assert await redis_backend.claim_task(external.id) is not None
    await redis_backend.set_execution_ref(external.id, "cloudrun", "jobs/legacy")
    terminal = await redis_backend.enqueue("tasks.maintenance.legacy.terminal")
    assert await redis_backend.claim_task(terminal.id) is not None
    assert await redis_backend.complete_task(terminal.id) is not None

    client = cast("Any", await redis_backend._get_client())
    await client.delete(
        redis_backend._maintenance_index_version_key,
        redis_backend._maintenance_running_key,
        redis_backend._maintenance_external_key,
        redis_backend._maintenance_terminal_key,
    )

    with pytest.raises(QueueConfigurationError, match="rebuild_maintenance_indexes"):
        await redis_backend.requeue_stale_running(stale_after=timedelta(seconds=-1), limit=1)
    with pytest.raises(QueueConfigurationError, match="rebuild_maintenance_indexes"):
        await redis_backend.list_running_external(limit=1)
    with pytest.raises(QueueConfigurationError, match="rebuild_maintenance_indexes"):
        await redis_backend.cleanup_terminal(datetime.now(timezone.utc) + timedelta(seconds=1), limit=1)

    assert await redis_backend.rebuild_maintenance_indexes() == 3
    assert await redis_backend.rebuild_maintenance_indexes() == 3
    assert [record.id for record in await redis_backend.list_running_external(limit=1)] == [external.id]
    assert (await redis_backend.requeue_stale_running(stale_after=timedelta(seconds=-1), limit=1)).requeued == 1
    assert await redis_backend.cleanup_terminal(datetime.now(timezone.utc) + timedelta(seconds=1), limit=1) == 1


async def test_redis_backend_coordination_expiry(redis_backend: "RedisQueueBackend") -> "None":
    await assert_coordination_expiry(redis_backend)


async def test_redis_backend_coordination_is_not_process_local(redis_service: "RedisService") -> "None":
    """Two independently opened Redis backends share the namespaced ownership key."""
    from litestar_queues.backends.redis import RedisBackendConfig, RedisQueueBackend

    prefix = f"litestar_queues:test:ownership:{uuid.uuid4().hex}"
    url = f"redis://{redis_service.host}:{redis_service.port}/{redis_service.db}"
    first = RedisQueueBackend(backend_config=RedisBackendConfig(url=url, key_prefix=prefix, worker_wakeups=False))
    second = RedisQueueBackend(backend_config=RedisBackendConfig(url=url, key_prefix=prefix, worker_wakeups=False))
    await first.open()
    await second.open()
    try:
        await assert_cross_instance_coordination(first, second)
    finally:
        await first.close()
        await second.close()

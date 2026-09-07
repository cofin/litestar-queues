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

    repair = await redis_backend.enqueue("tasks.maintenance.repair", execution_backend="cloudtasks")

    async def fail_full_status_scan(*_args: "Any", **_kwargs: "Any") -> "list[Any]":
        msg = "bounded maintenance enumerated a complete status set"
        raise AssertionError(msg)

    monkeypatch.setattr(type(redis_backend), "_list_records_by_statuses", fail_full_status_scan)

    assert [
        record.id for record in (await redis_backend.list_dispatch_repair_candidates("cloudtasks", limit=1)).records
    ] == [repair.id]
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


async def test_redis_dispatch_repair_upgrade_from_closed_backend(redis_backend: "RedisQueueBackend") -> "None":
    """An old populated namespace can be explicitly upgraded without opening it."""
    future = await redis_backend.enqueue(
        "repair.upgrade", execution_backend="cloudtasks", scheduled_at=datetime.now(timezone.utc) + timedelta(hours=1)
    )
    client = cast("Any", await redis_backend._get_client())
    await client.set(redis_backend._maintenance_index_version_key, "2")
    await redis_backend.close()
    with pytest.raises(QueueConfigurationError, match="rebuild_maintenance_indexes"):
        await redis_backend.open()
    await redis_backend.close()

    assert await redis_backend.rebuild_maintenance_indexes() == 1
    await redis_backend.close()
    await redis_backend.open()
    result = await redis_backend.list_dispatch_repair_candidates("cloudtasks", limit=2)
    assert [record.id for record in result.records] == [future.id]
    assert result.records[0].execution_ref is None
    assert result.records[0].scheduled_at == future.scheduled_at
    checked_at = result.records[0].dispatch_checked_at
    assert checked_at is not None
    assert await redis_backend.rebuild_maintenance_indexes() == 1
    stored = await redis_backend.get_task(future.id)
    assert stored is not None and stored.dispatch_checked_at == checked_at


async def test_redis_dispatch_repair_interrupted_rebuild_fails_closed(
    redis_backend: "RedisQueueBackend", monkeypatch: "pytest.MonkeyPatch"
) -> "None":
    """An interrupted rebuild invalidates even a previously current marker."""
    from litestar_queues.backends.redis import backend as backend_module

    await redis_backend.enqueue("repair.interrupted", execution_backend="cloudtasks")
    client = cast("Any", await redis_backend._get_client())
    original_execute = backend_module._execute_pipeline

    async def interrupt_pipeline(_pipeline: "Any") -> "Any":
        msg = "interrupted rebuild"
        raise RuntimeError(msg)

    monkeypatch.setattr(backend_module, "_execute_pipeline", interrupt_pipeline)
    with pytest.raises(RuntimeError, match="interrupted rebuild"):
        await redis_backend.rebuild_maintenance_indexes()
    assert await client.get(redis_backend._maintenance_index_version_key) is None
    with pytest.raises(QueueConfigurationError, match="rebuild_maintenance_indexes"):
        await redis_backend.list_dispatch_repair_candidates("cloudtasks", limit=1)

    monkeypatch.setattr(backend_module, "_execute_pipeline", original_execute)
    ghost_key = redis_backend._dispatch_repair_key("removed-backend")
    await client.sadd(redis_backend._dispatch_repair_registry_key, ghost_key)
    await client.zadd(ghost_key, {str(UUID(int=1)): 0})
    assert await redis_backend.rebuild_maintenance_indexes() == 1
    assert await client.exists(ghost_key) == 0
    assert (await redis_backend.list_dispatch_repair_candidates("cloudtasks", limit=1)).examined == 1


async def test_redis_dispatch_repair_stale_page_consumes_budget(redis_backend: "RedisQueueBackend") -> "None":
    """A full stale page never refills from eligible records beyond its limit."""
    live = await redis_backend.enqueue("repair.live", execution_backend="cloudtasks")
    client = cast("Any", await redis_backend._get_client())
    key = redis_backend._dispatch_repair_key("cloudtasks")
    await client.zadd(key, {str(UUID(int=1)): 0, str(UUID(int=2)): 0})
    result = await redis_backend.list_dispatch_repair_candidates("cloudtasks", limit=2)
    assert result.records == ()
    assert result.examined == 2
    assert result.limit_reached is True
    next_page = await redis_backend.list_dispatch_repair_candidates("cloudtasks", limit=2)
    assert [record.id for record in next_page.records] == [live.id]
    assert next_page.examined == 1
    assert next_page.limit_reached is False


async def test_redis_dispatch_checked_precision_and_backward_clock(
    redis_backend: "RedisQueueBackend", monkeypatch: "pytest.MonkeyPatch"
) -> "None":
    """Lua and rebuild retain fractional-millisecond scores and the newest mark."""
    from litestar_queues.backends.redis import backend as backend_module

    created = datetime.now(timezone.utc).replace(microsecond=123456)
    monkeypatch.setattr(backend_module, "_utc_now", lambda: created)
    high = await redis_backend.enqueue("repair.high", id=UUID(int=2), execution_backend="cloudtasks")
    low = await redis_backend.enqueue("repair.low", id=UUID(int=1), execution_backend="cloudtasks")
    checked = created + timedelta(seconds=1, microseconds=111)
    monkeypatch.setattr(backend_module, "_utc_now", lambda: checked)
    first = await redis_backend.list_dispatch_repair_candidates("cloudtasks", limit=1)
    assert [record.id for record in first.records] == [low.id]
    assert first.records[0].dispatch_checked_at == checked
    client = cast("Any", await redis_backend._get_client())
    key = redis_backend._dispatch_repair_key("cloudtasks")
    assert await client.zscore(key, str(low.id)) == checked.timestamp() * 1000
    second = await redis_backend.list_dispatch_repair_candidates("cloudtasks", limit=1)
    assert [record.id for record in second.records] == [high.id]

    monkeypatch.setattr(backend_module, "_utc_now", lambda: created)
    delayed = await redis_backend.list_dispatch_repair_candidates("cloudtasks", limit=2)
    assert all(record.dispatch_checked_at == checked for record in delayed.records)
    assert await redis_backend.rebuild_maintenance_indexes() == 2
    await redis_backend.close()
    await redis_backend.open()
    client = cast("Any", await redis_backend._get_client())
    assert await client.zscore(key, str(low.id)) == checked.timestamp() * 1000
    stored = await redis_backend.get_task(low.id)
    assert stored is not None and stored.dispatch_checked_at == checked


@pytest.mark.parametrize("transition", ["set_backend", "reserve", "release", "finalize"])
async def test_redis_dispatch_repair_follows_backend_changes(
    redis_backend: "RedisQueueBackend", transition: "str"
) -> "None":
    """Every backend-changing path moves membership and retains the check score."""
    record = await redis_backend.enqueue("repair.move", execution_backend="cloudtasks")
    selected = await redis_backend.list_dispatch_repair_candidates("cloudtasks", limit=1)
    checked_at = selected.records[0].dispatch_checked_at
    assert checked_at is not None
    if transition == "set_backend":
        changed = await redis_backend.set_execution_backend(record.id, "cloudrun")
    elif transition == "reserve":
        changed = await redis_backend.reserve_external_dispatch(record.id, "cloudrun", "reservation")
    else:
        assert await redis_backend.reserve_external_dispatch(record.id, "cloudtasks", "reservation") is not None
        if transition == "release":
            changed = await redis_backend.release_external_dispatch(record.id, "reservation", "cloudrun")
        else:
            changed = await redis_backend.finalize_external_dispatch(record.id, "reservation", "cloudrun", "delivery")
    assert changed is not None
    client = cast("Any", await redis_backend._get_client())
    assert await client.zscore(redis_backend._dispatch_repair_key("cloudtasks"), str(record.id)) is None
    assert await client.zscore(redis_backend._dispatch_repair_key("cloudrun"), str(record.id)) == (
        checked_at.timestamp() * 1000
    )
    assert (await redis_backend.list_dispatch_repair_candidates("cloudtasks", limit=1)).examined == 0
    assert [item.id for item in (await redis_backend.list_dispatch_repair_candidates("cloudrun", limit=1)).records] == [
        record.id
    ]


async def test_redis_dispatch_repair_keyed_retry_and_terminal_membership(redis_backend: "RedisQueueBackend") -> "None":
    """Keyed enqueue, claims, retries, cancellation and cleanup maintain the index."""
    record = await redis_backend.enqueue(
        "repair.keyed", key="repair-key", execution_backend="cloudtasks", max_retries=1
    )
    duplicate = await redis_backend.enqueue("repair.keyed", key="repair-key", execution_backend="cloudtasks")
    assert duplicate.id == record.id
    client = cast("Any", await redis_backend._get_client())
    key = redis_backend._dispatch_repair_key("cloudtasks")
    assert await client.zcard(key) == 1
    assert await redis_backend.claim_task(record.id) is not None
    assert await client.zcard(key) == 0
    assert await redis_backend.fail_task(record.id, error="retry") is not None
    assert await client.zcard(key) == 1
    assert await redis_backend.cancel_task(record.id) is not None
    assert await client.zcard(key) == 0
    assert await redis_backend.cleanup_terminal(datetime.now(timezone.utc) + timedelta(seconds=1), limit=1) == 1
    assert await client.zcard(key) == 0


async def test_redis_dispatch_repair_nonpositive_limit_does_not_open(
    redis_backend: "RedisQueueBackend", monkeypatch: "pytest.MonkeyPatch"
) -> "None":
    """Validation and empty requests require no storage connection."""
    await redis_backend.close()

    async def unexpected_client(_self: "Any") -> "Any":
        msg = "nonpositive limit opened storage"
        raise AssertionError(msg)

    monkeypatch.setattr(type(redis_backend), "_get_client", unexpected_client)
    result = await redis_backend.list_dispatch_repair_candidates("cloudtasks", limit=0)
    assert result.records == () and result.examined == 0 and result.limit_reached is False
    with pytest.raises(QueueConfigurationError, match="non-negative"):
        await redis_backend.list_dispatch_repair_candidates("cloudtasks", limit=-1)


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

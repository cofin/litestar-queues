"""Valkey distributed maintenance coordination and bounded-operation contract."""

import uuid
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, cast

import pytest

pytest.importorskip("valkey")

from litestar_queues.exceptions import QueueConfigurationError
from tests.integration.backends._maintenance_asserts import (
    assert_bounded_cleanup_terminal,
    assert_bounded_stale_recovery,
    assert_coordination_expiry,
    assert_cross_instance_coordination,
)

if TYPE_CHECKING:
    from litestar_queues.backends.valkey import ValkeyQueueBackend
    from tests.integration.backends.valkey.conftest import ValkeyService

pytestmark = pytest.mark.anyio


async def test_valkey_backend_bounded_cleanup_terminal(valkey_backend: "ValkeyQueueBackend") -> "None":
    await assert_bounded_cleanup_terminal(valkey_backend)


async def test_valkey_backend_bounded_stale_recovery(valkey_backend: "ValkeyQueueBackend") -> "None":
    await assert_bounded_stale_recovery(valkey_backend)


async def test_valkey_bounded_maintenance_does_not_enumerate_status_sets(
    valkey_backend: "ValkeyQueueBackend", monkeypatch: "pytest.MonkeyPatch"
) -> "None":
    """Valkey inherits the same server-bounded ordered maintenance path."""
    external = await valkey_backend.enqueue("tasks.maintenance.external", execution_backend="cloudrun")
    claimed_external = await valkey_backend.claim_task(external.id)
    assert claimed_external is not None
    await valkey_backend.set_execution_ref(external.id, "cloudrun", "jobs/external")

    stale = await valkey_backend.enqueue("tasks.maintenance.stale", max_retries=1)
    assert await valkey_backend.claim_task(stale.id) is not None

    terminal = await valkey_backend.enqueue("tasks.maintenance.terminal")
    assert await valkey_backend.claim_task(terminal.id) is not None
    assert await valkey_backend.complete_task(terminal.id) is not None

    repair = await valkey_backend.enqueue("tasks.maintenance.repair", execution_backend="cloudtasks")

    async def fail_full_status_scan(*_args: "Any", **_kwargs: "Any") -> "list[Any]":
        msg = "bounded maintenance enumerated a complete status set"
        raise AssertionError(msg)

    monkeypatch.setattr(type(valkey_backend), "_list_records_by_statuses", fail_full_status_scan)

    assert [
        record.id for record in (await valkey_backend.list_dispatch_repair_candidates("cloudtasks", limit=1)).records
    ] == [repair.id]
    assert [record.id for record in await valkey_backend.list_running_external(limit=1)] == [external.id]
    stale_result = await valkey_backend.requeue_stale_running(stale_after=timedelta(seconds=-2), limit=1)
    assert stale_result.requeued + stale_result.failed == 1
    assert await valkey_backend.cleanup_terminal(datetime.now(timezone.utc) + timedelta(seconds=1), limit=1) == 1


async def test_valkey_bounded_maintenance_fails_closed_until_legacy_indexes_are_rebuilt(
    valkey_backend: "ValkeyQueueBackend",
) -> "None":
    """Valkey shares Redis' explicit populated-namespace upgrade gate."""
    records = []
    for index in range(3):
        record = await valkey_backend.enqueue(f"tasks.maintenance.legacy.{index}", max_retries=1)
        assert await valkey_backend.claim_task(record.id) is not None
        records.append(record)

    client = cast("Any", await valkey_backend._get_client())
    await client.delete(
        valkey_backend._maintenance_index_version_key,
        valkey_backend._maintenance_running_key,
        valkey_backend._maintenance_external_key,
        valkey_backend._maintenance_terminal_key,
    )

    with pytest.raises(QueueConfigurationError, match="rebuild_maintenance_indexes"):
        await valkey_backend.requeue_stale_running(stale_after=timedelta(seconds=-1), limit=2)

    assert await valkey_backend.rebuild_maintenance_indexes() == 3
    assert await valkey_backend.rebuild_maintenance_indexes() == 3
    assert (await valkey_backend.requeue_stale_running(stale_after=timedelta(seconds=-1), limit=2)).requeued == 2
    assert (await valkey_backend.requeue_stale_running(stale_after=timedelta(seconds=-1), limit=2)).requeued == 1


async def test_valkey_backend_coordination_expiry(valkey_backend: "ValkeyQueueBackend") -> "None":
    await assert_coordination_expiry(valkey_backend)


async def test_valkey_dispatch_repair_upgrade_from_closed_backend(valkey_backend: "ValkeyQueueBackend") -> "None":
    """Valkey shares the explicit upgrade path for old NULL-reference records."""
    record = await valkey_backend.enqueue("repair.upgrade", execution_backend="cloudtasks")
    client = cast("Any", await valkey_backend._get_client())
    await client.set(valkey_backend._maintenance_index_version_key, "2")
    await valkey_backend.close()
    with pytest.raises(QueueConfigurationError, match="rebuild_maintenance_indexes"):
        await valkey_backend.open()
    await valkey_backend.close()
    assert await valkey_backend.rebuild_maintenance_indexes() == 1
    await valkey_backend.close()
    await valkey_backend.open()
    result = await valkey_backend.list_dispatch_repair_candidates("cloudtasks", limit=2)
    assert [item.id for item in result.records] == [record.id]
    assert result.records[0].execution_ref is None
    assert result.examined == 1
    assert result.limit_reached is False


async def test_valkey_dispatch_repair_stale_page_consumes_budget(valkey_backend: "ValkeyQueueBackend") -> "None":
    """Inherited Lua must count deleted hashes without refilling the page."""
    record = await valkey_backend.enqueue("repair.live", execution_backend="cloudtasks")
    client = cast("Any", await valkey_backend._get_client())
    await client.zadd(valkey_backend._dispatch_repair_key("cloudtasks"), {str(uuid.UUID(int=1)): 0})
    result = await valkey_backend.list_dispatch_repair_candidates("cloudtasks", limit=1)
    assert result.records == ()
    assert result.examined == 1
    assert result.limit_reached is True
    assert [
        item.id for item in (await valkey_backend.list_dispatch_repair_candidates("cloudtasks", limit=1)).records
    ] == [record.id]


async def test_valkey_backend_coordination_is_not_process_local(valkey_service: "ValkeyService") -> "None":
    """Two independently opened Valkey backends share the namespaced ownership key."""
    from litestar_queues.backends.valkey import ValkeyBackendConfig, ValkeyQueueBackend

    prefix = f"litestar_queues:test:ownership:{uuid.uuid4().hex}"
    url = f"redis://{valkey_service.host}:{valkey_service.port}/{valkey_service.db}"
    first = ValkeyQueueBackend(backend_config=ValkeyBackendConfig(url=url, key_prefix=prefix, worker_wakeups=False))
    second = ValkeyQueueBackend(backend_config=ValkeyBackendConfig(url=url, key_prefix=prefix, worker_wakeups=False))
    await first.open()
    await second.open()
    try:
        await assert_cross_instance_coordination(first, second)
    finally:
        await first.close()
        await second.close()

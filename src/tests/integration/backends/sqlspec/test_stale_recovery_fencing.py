"""SQLSpec stale-recovery fencing regression tests."""

import contextlib
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

import pytest

pytest.importorskip("aiosqlite")
pytest.importorskip("sqlspec")

from litestar_queues.backends.sqlspec import SQLSpecQueueBackend

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable
    from uuid import UUID

pytestmark = pytest.mark.anyio


async def test_sqlspec_stale_recovery_does_not_requeue_task_completed_after_stale_select(
    sqlspec_backend: "SQLSpecQueueBackend", monkeypatch: "pytest.MonkeyPatch"
) -> "None":
    record = await sqlspec_backend.enqueue("tasks.stale.completed", max_retries=2)
    claimed = await sqlspec_backend.claim_task(record.id)

    assert claimed is not None

    completed = False
    original_session = SQLSpecQueueBackend._session

    async def complete_after_stale_select(driver: "Any") -> "None":
        nonlocal completed
        if completed:
            return
        completed = True
        await _complete_task_on_driver(sqlspec_backend, driver, claimed.id)

    @contextlib.asynccontextmanager
    async def interleaving_session(self: "SQLSpecQueueBackend") -> "AsyncIterator[Any]":
        async with original_session(self) as driver:
            if self is sqlspec_backend:
                yield _SelectHookDriver(driver, complete_after_stale_select)
            else:
                yield driver

    monkeypatch.setattr(SQLSpecQueueBackend, "_session", interleaving_session)

    recovery = await sqlspec_backend.requeue_stale_running(stale_after=timedelta(seconds=-1))
    stored = await sqlspec_backend.get_task(record.id)

    assert recovery.requeued == 0
    assert recovery.failed == 0
    assert stored is not None
    assert stored.status == "completed"
    assert stored.retry_count == claimed.retry_count


async def test_sqlspec_expected_retry_count_prevents_stale_owner_from_failing_reclaimed_task(
    sqlspec_backend: "SQLSpecQueueBackend", monkeypatch: "pytest.MonkeyPatch"
) -> "None":
    record = await sqlspec_backend.enqueue("tasks.stale.owner", max_retries=2)
    claimed = await sqlspec_backend.claim_task(record.id)

    assert claimed is not None

    interleaved = False
    original_select_task = SQLSpecQueueBackend._select_task

    async def select_task_with_reclaim(
        self: "SQLSpecQueueBackend", driver: "Any", task_id: "UUID"
    ) -> "dict[str, Any] | None":
        nonlocal interleaved
        row = await original_select_task(self, driver, task_id)
        if self is sqlspec_backend and task_id == claimed.id and not interleaved:
            interleaved = True
            await _requeue_and_claim_on_driver(self, driver, claimed.id, claimed.retry_count)
        return row

    monkeypatch.setattr(SQLSpecQueueBackend, "_select_task", select_task_with_reclaim)

    stale_failure = await sqlspec_backend.fail_task(
        claimed.id, "stale owner failed", retry=False, expected_retry_count=claimed.retry_count
    )
    stored = await sqlspec_backend.get_task(record.id)

    assert stale_failure is None
    assert stored is not None
    assert stored.status == "running"
    assert stored.retry_count == claimed.retry_count + 1
    assert stored.error == "Task heartbeat stale"


class _SelectHookDriver:
    def __init__(self, driver: "Any", after_select: "Callable[[Any], Awaitable[None]]") -> "None":
        self._driver = driver
        self._after_select = after_select

    def __getattr__(self, name: "str") -> "Any":
        return getattr(self._driver, name)

    async def select(self, *args: "Any", **kwargs: "Any") -> "Any":
        rows = await self._driver.select(*args, **kwargs)
        if rows:
            await self._after_select(self._driver)
        return rows


async def _complete_task_on_driver(backend: "SQLSpecQueueBackend", driver: "Any", task_id: "UUID") -> "None":
    store = backend._get_store()
    now = backend._serialize_datetime(datetime.now(timezone.utc))
    try:
        await driver.execute(
            store.complete_task(
                task_id=str(task_id),
                completed_at=now,
                heartbeat_at=now,
                result_json=store.serialize_json("result_json", {"ok": True}),
            )
        )
        await driver.commit()
    except Exception:
        with contextlib.suppress(Exception):
            await driver.rollback()
        raise


async def _requeue_and_claim_on_driver(
    backend: "SQLSpecQueueBackend", driver: "Any", task_id: "UUID", retry_count: "int"
) -> "None":
    store = backend._get_store()
    now = backend._serialize_datetime(datetime.now(timezone.utc))
    await driver.execute(
        store.retry_task(task_id=str(task_id), error="Task heartbeat stale", retry_count=retry_count + 1)
    )
    await driver.execute(store.claim_task(task_id=str(task_id), due_at=now, started_at=now, heartbeat_at=now))

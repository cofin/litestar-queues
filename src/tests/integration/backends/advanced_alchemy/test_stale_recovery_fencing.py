"""Advanced Alchemy stale-recovery fencing regression tests."""

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

import pytest

pytest.importorskip("advanced_alchemy")
pytest.importorskip("aiosqlite")
pytest.importorskip("sqlalchemy")

from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from litestar_queues.backends.advanced_alchemy.service import QueueTaskService, _serialize_json

if TYPE_CHECKING:
    from uuid import UUID

    from litestar_queues.backends.advanced_alchemy import SQLAlchemyBackend

pytestmark = pytest.mark.anyio


async def test_advanced_alchemy_stale_recovery_does_not_requeue_task_completed_after_stale_select(
    advanced_alchemy_backend: "SQLAlchemyBackend", monkeypatch: "pytest.MonkeyPatch"
) -> "None":
    record = await advanced_alchemy_backend.enqueue("tasks.stale.completed", max_retries=2)
    claimed = await advanced_alchemy_backend.claim_task(record.id)

    assert claimed is not None

    completed = False
    model_type = advanced_alchemy_backend._model_class
    original_execute = AsyncSession.execute

    async def execute_with_completion(self: "AsyncSession", statement: "Any", *args: "Any", **kwargs: "Any") -> "Any":
        nonlocal completed
        result = await original_execute(self, statement, *args, **kwargs)
        if not completed and _selects_model(statement, model_type):
            completed = True
            await _complete_task_on_session(self, model_type, claimed.id)
        return result

    monkeypatch.setattr(AsyncSession, "execute", execute_with_completion)

    recovery = await advanced_alchemy_backend.requeue_stale_running(stale_after=timedelta(seconds=-1))
    stored = await advanced_alchemy_backend.get_task(record.id)

    assert recovery.requeued == 0
    assert recovery.failed == 0
    assert stored is not None
    assert stored.status == "completed"
    assert stored.retry_count == claimed.retry_count


async def test_advanced_alchemy_expected_retry_count_prevents_stale_owner_from_failing_reclaimed_task(
    advanced_alchemy_backend: "SQLAlchemyBackend", monkeypatch: "pytest.MonkeyPatch"
) -> "None":
    record = await advanced_alchemy_backend.enqueue("tasks.stale.owner", max_retries=2)
    claimed = await advanced_alchemy_backend.claim_task(record.id)

    assert claimed is not None

    interleaved = False
    original_select_task = QueueTaskService._select_task

    async def select_task_with_reclaim(self: "QueueTaskService", task_id: "UUID") -> "Any | None":
        nonlocal interleaved
        model = await original_select_task(self, task_id)
        if task_id == claimed.id and not interleaved:
            interleaved = True
            await _requeue_and_claim_on_session(
                self.repository.session, self.model_type, claimed.id, claimed.retry_count
            )
        return model

    monkeypatch.setattr(QueueTaskService, "_select_task", select_task_with_reclaim)

    stale_failure = await advanced_alchemy_backend.fail_task(
        claimed.id, "stale owner failed", retry=False, expected_retry_count=claimed.retry_count
    )
    stored = await advanced_alchemy_backend.get_task(record.id)

    assert stale_failure is None
    assert stored is not None
    assert stored.status == "running"
    assert stored.retry_count == claimed.retry_count + 1
    assert stored.error == "Task heartbeat stale"


def _selects_model(statement: "Any", model_type: "type[Any]") -> "bool":
    if not getattr(statement, "is_select", False):
        return False
    return any(description.get("entity") is model_type for description in getattr(statement, "column_descriptions", ()))


async def _complete_task_on_session(session: "AsyncSession", model_type: "type[Any]", task_id: "UUID") -> "None":
    now = datetime.now(timezone.utc)
    await session.execute(
        update(model_type)
        .where(model_type.id == task_id)
        .values(
            status="completed",
            completed_at=now,
            heartbeat_at=now,
            result_json=_serialize_json({"ok": True}),
            error=None,
        )
        .execution_options(synchronize_session=False)
    )


async def _requeue_and_claim_on_session(
    session: "AsyncSession", model_type: "type[Any]", task_id: "UUID", retry_count: "int"
) -> "None":
    now = datetime.now(timezone.utc)
    await session.execute(
        update(model_type)
        .where(model_type.id == task_id)
        .values(
            status="pending",
            started_at=None,
            heartbeat_at=None,
            retry_count=retry_count + 1,
            error="Task heartbeat stale",
        )
        .execution_options(synchronize_session=False)
    )
    await session.execute(
        update(model_type)
        .where(model_type.id == task_id)
        .values(status="running", started_at=now, heartbeat_at=now)
        .execution_options(synchronize_session=False)
    )

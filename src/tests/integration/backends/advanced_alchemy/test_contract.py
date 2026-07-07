"""Advanced Alchemy queue-backend contract suite.

Smoke tests (registration + import isolation + schema bootstrap config) stay
unparametrized. The 6 behavior tests consume the ``advanced_alchemy_backend``
fixture parametrized over ``AA_ENGINES`` (aiosqlite + Postgres + MySQL +
Oracle async configs) by the subdir conftest.
"""

import sys
from datetime import datetime, timedelta, timezone
from subprocess import run
from typing import TYPE_CHECKING, Any, cast
from uuid import uuid4

import pytest
from sqlalchemy.dialects import mysql, oracle, postgresql, sqlite

pytest.importorskip("advanced_alchemy")
pytest.importorskip("aiosqlite")
pytest.importorskip("sqlalchemy")

from advanced_alchemy.base import UUIDAuditBase
from advanced_alchemy.extensions.litestar import SQLAlchemyAsyncConfig
from advanced_alchemy.operations import MergeStatement

from litestar_queues import HeartbeatTouch, QueueConfig, QueueService, task
from litestar_queues.backends import get_queue_backend_class, list_queue_backends
from litestar_queues.backends.advanced_alchemy import (
    AdvancedAlchemyBackendConfig,
    AdvancedAlchemyQueueBackend,
    QueueTaskModelMixin,
)
from litestar_queues.models import QueuedTaskRecord
from litestar_queues.task import clear_task_registry

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.anyio


@pytest.fixture(autouse=True)
def clean_task_registry() -> "None":
    clear_task_registry()


class ContractQueueTask(UUIDAuditBase, QueueTaskModelMixin):
    __tablename__ = "aa_contract_queue_tasks"


async def test_advanced_alchemy_backend_is_registered_without_sqlspec() -> "None":
    assert "advanced-alchemy" in list_queue_backends()
    assert get_queue_backend_class("advanced-alchemy") is AdvancedAlchemyQueueBackend


def test_top_level_litestar_queues_import_does_not_require_advanced_alchemy() -> "None":
    """Importing ``litestar_queues`` must succeed without advanced_alchemy installed."""
    code = """
import builtins

blocked_prefixes = ("advanced_alchemy", "sqlalchemy")
blocked_package_prefixes = tuple(f"{name}." for name in blocked_prefixes)
original_import = builtins.__import__

def blocked_import(name, *args, **kwargs):
    if name in blocked_prefixes or name.startswith(blocked_package_prefixes):
        raise ModuleNotFoundError(name)
    return original_import(name, *args, **kwargs)

builtins.__import__ = blocked_import

import litestar_queues
from litestar_queues import InMemoryQueueBackend, QueueConfig

assert "InMemoryQueueBackend" in litestar_queues.__all__
assert "AdvancedAlchemyQueueBackend" not in litestar_queues.__all__
assert QueueConfig().queue_backend == "memory"
"""
    result = run([sys.executable, "-c", code], capture_output=True, text=True, check=False)

    assert result.returncode == 0, result.stderr


async def test_advanced_alchemy_backend_exposes_schema_bootstrap_config(tmp_path: "Path") -> "None":
    backend_config = AdvancedAlchemyBackendConfig(
        sqlalchemy_config=_sqlite_config(tmp_path / "configured.db"), model_class=ContractQueueTask, create_schema=True
    )

    assert backend_config.model_class is ContractQueueTask
    assert backend_config.create_schema is True


def test_advanced_alchemy_mixin_uses_native_json_column_types() -> "None":
    column_type = ContractQueueTask.__table__.c.args_json.type

    assert column_type.compile(dialect=_postgresql_dialect()) == "JSONB"
    assert column_type.compile(dialect=_sqlite_dialect()) == "JSON"


def test_advanced_alchemy_claim_statement_uses_skip_locked_for_locking_dialects() -> "None":
    from litestar_queues.backends.advanced_alchemy.service import (
        _build_claim_candidate_statement,
        _build_claim_lock_statement,
        _supports_skip_locked_claim,
    )

    claim_time = datetime.now(timezone.utc)
    postgres_statement = _build_claim_candidate_statement(
        ContractQueueTask,
        queue=None,
        execution_backend=None,
        now=claim_time,
        limit=1,
        skip_locked=_supports_skip_locked_claim("postgresql"),
    )
    sqlite_statement = _build_claim_candidate_statement(
        ContractQueueTask,
        queue=None,
        execution_backend=None,
        now=claim_time,
        limit=1,
        skip_locked=_supports_skip_locked_claim("sqlite"),
    )
    oracle_statement = _build_claim_candidate_statement(
        ContractQueueTask, queue=None, execution_backend=None, now=claim_time, limit=10, skip_locked=False
    )
    oracle_lock_statement = _build_claim_lock_statement(ContractQueueTask, uuid4())

    assert "FOR UPDATE SKIP LOCKED" in str(postgres_statement.compile(dialect=_postgresql_dialect()))
    assert "FOR UPDATE" not in str(sqlite_statement.compile(dialect=_sqlite_dialect()))
    oracle_sql = str(oracle_statement.compile(dialect=_oracle_dialect()))
    oracle_lock_sql = str(oracle_lock_statement.compile(dialect=_oracle_dialect()))
    assert "FOR UPDATE" not in oracle_sql
    assert "FETCH FIRST" in oracle_sql
    assert "FOR UPDATE SKIP LOCKED" in oracle_lock_sql
    assert not _supports_skip_locked_claim("mysql")
    assert not _supports_skip_locked_claim("mariadb")


async def test_advanced_alchemy_cas_claim_scans_past_first_failed_batch() -> "None":
    from litestar_queues.backends.advanced_alchemy.service import QueueTaskService

    records = [QueuedTaskRecord(task_name=f"tasks.cas.{index}") for index in range(11)]
    target = records[-1]
    service = _CasClaimService(records, target)

    claimed = await QueueTaskService.claim_next(cast("QueueTaskService", service), queue=None, execution_backend=None)

    assert claimed is target
    assert service.list_limits == [10, 20]


def test_advanced_alchemy_keyed_enqueue_uses_native_upsert_for_supported_dialects() -> "None":
    from litestar_queues.backends.advanced_alchemy.service import (
        _build_keyed_enqueue_upsert,
        _supports_native_keyed_enqueue,
    )

    values = {
        "id": uuid4(),
        "task_name": "tasks.native_upsert",
        "task_key": "native:upsert",
        "args_json": [],
        "kwargs_json": {},
        "metadata_json": {},
    }

    postgres_statement, postgres_params = _build_keyed_enqueue_upsert(
        ContractQueueTask.__table__, values, dialect_name="postgresql"
    )
    mysql_statement, mysql_params = _build_keyed_enqueue_upsert(
        ContractQueueTask.__table__, values, dialect_name="mysql"
    )
    oracle_statement, oracle_params = _build_keyed_enqueue_upsert(
        ContractQueueTask.__table__, values, dialect_name="oracle"
    )

    assert _supports_native_keyed_enqueue("postgresql")
    assert not _supports_native_keyed_enqueue("sqlite")
    assert "ON CONFLICT" in str(postgres_statement.compile(dialect=_postgresql_dialect()))
    assert "ON DUPLICATE KEY UPDATE" in str(mysql_statement.compile(dialect=_mysql_dialect()))
    assert isinstance(oracle_statement, MergeStatement)
    assert "MERGE INTO" in str(oracle_statement.compile(dialect=_oracle_dialect()))
    assert postgres_params == {}
    assert mysql_params == {}
    assert set(oracle_params).issubset(ContractQueueTask.__table__.c)


async def test_advanced_alchemy_backend_deduplicates_active_keys_and_replaces_terminal_keys(
    advanced_alchemy_backend: "AdvancedAlchemyQueueBackend",
) -> "None":
    first = await advanced_alchemy_backend.enqueue("tasks.sync", kwargs={"account_id": "acct-1"}, key="sync:acct-1")
    duplicate = await advanced_alchemy_backend.enqueue("tasks.sync", kwargs={"account_id": "acct-2"}, key="sync:acct-1")

    assert duplicate.id == first.id
    assert duplicate.kwargs == {"account_id": "acct-1"}

    await advanced_alchemy_backend.complete_task(first.id, result={"ok": True})
    replacement = await advanced_alchemy_backend.enqueue(
        "tasks.sync", kwargs={"account_id": "acct-2"}, key="sync:acct-1"
    )

    assert replacement.id != first.id
    assert replacement.kwargs == {"account_id": "acct-2"}
    keyed = await advanced_alchemy_backend.get_task_by_key("sync:acct-1")
    assert keyed is not None
    assert keyed.id == replacement.id


async def test_advanced_alchemy_backend_claims_due_tasks_by_priority(
    advanced_alchemy_backend: "AdvancedAlchemyQueueBackend",
) -> "None":
    later = datetime.now(timezone.utc) + timedelta(minutes=5)

    low = await advanced_alchemy_backend.enqueue("tasks.low", priority=1)
    scheduled = await advanced_alchemy_backend.enqueue("tasks.later", priority=100, scheduled_at=later)
    high = await advanced_alchemy_backend.enqueue("tasks.high", priority=10)

    claimed = await advanced_alchemy_backend.claim_next()

    assert claimed is not None
    assert claimed.id == high.id
    assert claimed.status == "running"
    assert claimed.started_at is not None
    stored_low = await advanced_alchemy_backend.get_task(low.id)
    stored_scheduled = await advanced_alchemy_backend.get_task(scheduled.id)
    assert stored_low is not None
    assert stored_scheduled is not None
    assert stored_low.status == "pending"
    assert stored_scheduled.status == "scheduled"


async def test_advanced_alchemy_backend_fail_task_retries_then_fails_permanently(
    advanced_alchemy_backend: "AdvancedAlchemyQueueBackend",
) -> "None":
    record = await advanced_alchemy_backend.enqueue("tasks.flaky", max_retries=1)

    await advanced_alchemy_backend.claim_task(record.id)
    retried = await advanced_alchemy_backend.fail_task(record.id, "first failure")

    assert retried is not None
    assert retried.status == "pending"
    assert retried.retry_count == 1

    await advanced_alchemy_backend.claim_task(record.id)
    failed = await advanced_alchemy_backend.fail_task(record.id, "second failure")

    assert failed is not None
    assert failed.status == "failed"
    assert failed.error == "second failure"
    assert failed.completed_at is not None


async def test_advanced_alchemy_backend_preserves_json_string_results(
    advanced_alchemy_backend: "AdvancedAlchemyQueueBackend",
) -> "None":
    record = await advanced_alchemy_backend.enqueue("tasks.string-result")
    claimed = await advanced_alchemy_backend.claim_task(record.id)

    assert claimed is not None
    completed = await advanced_alchemy_backend.complete_task(claimed.id, result="123")
    stored = await advanced_alchemy_backend.get_task(record.id)

    assert completed is not None
    assert completed.result == "123"
    assert stored is not None
    assert stored.result == "123"


async def test_advanced_alchemy_backend_cancels_heartbeats_and_requeues_stale_running(
    advanced_alchemy_backend: "AdvancedAlchemyQueueBackend",
) -> "None":
    pending = await advanced_alchemy_backend.enqueue("tasks.cancel")

    assert await advanced_alchemy_backend.cancel_task(pending.id)
    assert not await advanced_alchemy_backend.cancel_task(pending.id)

    cancelled = await advanced_alchemy_backend.get_task(pending.id)
    assert cancelled is not None
    assert cancelled.status == "cancelled"

    running = await advanced_alchemy_backend.enqueue("tasks.heartbeat", max_retries=1, metadata={"existing": "kept"})
    claimed = await advanced_alchemy_backend.claim_task(running.id)

    assert claimed is not None
    assert claimed.heartbeat_at is not None

    result = await advanced_alchemy_backend.touch_heartbeats([
        HeartbeatTouch(
            task_id=claimed.id, expected_retry_count=claimed.retry_count, metadata_patch={"progress_detail": "row 5"}
        )
    ])
    touched = await advanced_alchemy_backend.get_task(claimed.id)

    assert result.touched_task_ids == {claimed.id}
    assert result.missed_task_ids == set()
    assert touched is not None
    assert touched.heartbeat_at is not None
    assert touched.heartbeat_at >= claimed.heartbeat_at
    assert touched.metadata == {"existing": "kept", "progress_detail": "row 5"}

    stale_result = await advanced_alchemy_backend.requeue_stale_running(stale_after=timedelta(seconds=0))
    assert stale_result.requeued == 1
    requeued = await advanced_alchemy_backend.get_task(claimed.id)

    assert requeued is not None
    assert requeued.status == "pending"
    assert requeued.retry_count == 1

    exhausted = await advanced_alchemy_backend.enqueue("tasks.exhausted", max_retries=0)
    exhausted_claim = await advanced_alchemy_backend.claim_task(exhausted.id)
    assert exhausted_claim is not None
    exhausted_result = await advanced_alchemy_backend.requeue_stale_running(stale_after=timedelta(seconds=0))
    exhausted_stored = await advanced_alchemy_backend.get_task(exhausted.id)

    assert exhausted_result.failed == 1
    assert exhausted_stored is not None
    assert exhausted_stored.status == "failed"
    assert exhausted_stored.error == "Task heartbeat stale"


async def test_advanced_alchemy_backend_operational_queries_and_execution_refs(
    advanced_alchemy_backend: "AdvancedAlchemyQueueBackend",
) -> "None":
    local = await advanced_alchemy_backend.enqueue("tasks.local", priority=100, execution_backend="local")
    external = await advanced_alchemy_backend.enqueue(
        "tasks.remote", execution_backend="cloudrun", execution_profile="batch-small"
    )
    completed = await advanced_alchemy_backend.enqueue(
        "tasks.report", metadata={"encoded_at": datetime.now(timezone.utc)}
    )

    pending = await advanced_alchemy_backend.list_pending(limit=10, execution_backend="cloudrun")
    claimed = await advanced_alchemy_backend.claim_next(execution_backend="cloudrun")
    completed_claim = await advanced_alchemy_backend.claim_task(completed.id)

    assert [record.id for record in pending] == [external.id]
    assert claimed is not None
    assert claimed.id == external.id
    assert completed_claim is not None

    await advanced_alchemy_backend.set_execution_ref(
        claimed.id, "cloudrun", "jobs/abc-123", execution_profile="batch-small"
    )
    await advanced_alchemy_backend.complete_task(completed_claim.id, result={"ok": True})

    running_external = await advanced_alchemy_backend.list_running_external()
    stored_local = await advanced_alchemy_backend.get_task(local.id)
    statistics = await advanced_alchemy_backend.get_statistics()
    completed_records = await advanced_alchemy_backend.list_completed_by_task("tasks.report")
    cleanup_count = await advanced_alchemy_backend.cleanup_terminal(datetime.now(timezone.utc) + timedelta(seconds=1))

    assert [record.id for record in running_external] == [external.id]
    assert running_external[0].execution_ref == "jobs/abc-123"
    assert stored_local is not None
    assert stored_local.status == "pending"
    assert statistics.pending == 1
    assert statistics.running == 1
    assert statistics.completed == 1
    assert completed_records[0].metadata["encoded_at"].endswith("Z")
    assert cleanup_count == 1
    assert await advanced_alchemy_backend.get_task(completed.id) is None


async def test_queue_service_uses_advanced_alchemy_backend(tmp_path: "Path") -> "None":
    @task("tasks.aa")
    async def aa_task() -> "str":
        return "ok"

    queue_config = QueueConfig(
        queue_backend=AdvancedAlchemyBackendConfig(
            sqlalchemy_config=_sqlite_config(tmp_path / "service.db"), model_class=ContractQueueTask, create_schema=True
        )
    )

    async with QueueService(queue_config) as service:
        result = await service.enqueue("tasks.aa", execution_backend="local")
        stored = await service.get_task(result.id)

    assert result.status == "pending"
    assert stored is not None
    assert stored.task_name == "tasks.aa"


def _sqlite_config(path: "Path") -> "SQLAlchemyAsyncConfig":
    return SQLAlchemyAsyncConfig(connection_string=f"sqlite+aiosqlite:///{path}")


def _postgresql_dialect() -> "Any":
    return postgresql.dialect()  # type: ignore[no-untyped-call]


def _sqlite_dialect() -> "Any":
    return sqlite.dialect()


def _mysql_dialect() -> "Any":
    return mysql.dialect()


def _oracle_dialect() -> "Any":
    return oracle.dialect()  # type: ignore[no-untyped-call]


class _CasClaimService:
    def __init__(self, records: "list[QueuedTaskRecord]", target: "QueuedTaskRecord") -> "None":
        self.records = records
        self.target = target
        self.list_limits: "list[int]" = []

    def _dialect_name(self) -> "str":
        return "sqlite"

    async def list_pending(
        self, *, limit: "int", queue: "str | None", execution_backend: "str | None"
    ) -> "list[QueuedTaskRecord]":
        del queue, execution_backend
        self.list_limits.append(limit)
        return self.records[:limit]

    async def claim_task(self, task_id: "object") -> "QueuedTaskRecord | None":
        if task_id == self.target.id:
            return self.target
        return None

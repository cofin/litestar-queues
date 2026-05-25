"""Tests for SQLSpec column remapping and adopter-owned tables."""

import importlib
import sqlite3
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import TYPE_CHECKING

import pytest

pytest.importorskip("aiosqlite")
pytest.importorskip("sqlspec")

from sqlspec.adapters.aiosqlite import AiosqliteConfig

from litestar_queues.backends.sqlspec import SQLSpecBackendConfig, SQLSpecQueueBackend
from litestar_queues.backends.sqlspec.store import create_queue_store
from litestar_queues.exceptions import QueueConfigurationError

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.anyio

ADOPTER_COLUMN_MAP = {
    "id": "task_id",
    "task_name": "function",
    "args_json": "args",
    "kwargs_json": "data",
    "queue": "lane",
    "execution_backend": "execution_target",
    "execution_profile": "profile",
    "execution_ref": "external_ref",
    "status": "state",
    "priority": "importance",
    "max_retries": "attempt_limit",
    "retry_count": "attempt_count",
    "scheduled_at": "run_after",
    "created_at": "created",
    "started_at": "started",
    "completed_at": "finished",
    "heartbeat_at": "heartbeat",
    "result_json": "outcome",
    "error": "failure",
    "task_key": "key",
    "metadata_json": "meta",
}


def test_column_map_resolves_generated_statement_columns(
    tmp_path: "Path",
    sqlite_config_factory: Callable[["Path"], AiosqliteConfig],
) -> None:
    """Every statement variant uses mapped columns while SELECT aliases preserve canonical rows."""
    store = create_queue_store(
        sqlite_config_factory(tmp_path / "statements.db"),
        table_name="adopter_jobs",
        column_map=ADOPTER_COLUMN_MAP,
    )

    insert_sql = store.insert_task({"id": "task-1", "task_name": "tasks.sync"}).build(dialect="sqlite").sql
    select_sql = store.select_task("task-1").build(dialect="sqlite").sql
    pending_sql = store.list_pending(now=datetime.now(UTC).isoformat(), limit=10, queue="critical").build(
        dialect="sqlite"
    ).sql
    claim_sql = store.claim_task(
        task_id="task-1",
        due_at=datetime.now(UTC).isoformat(),
        heartbeat_at=datetime.now(UTC).isoformat(),
        started_at=datetime.now(UTC).isoformat(),
    ).build(dialect="sqlite").sql
    complete_sql = store.complete_task(
        task_id="task-1",
        completed_at=datetime.now(UTC).isoformat(),
        heartbeat_at=datetime.now(UTC).isoformat(),
        result_json='{"ok": true}',
    ).build(dialect="sqlite").sql
    stale_sql = store.requeue_stale(cutoff=datetime.now(UTC).isoformat()).build(dialect="sqlite").sql
    external_sql = store.list_running_external(limit=10).build(dialect="sqlite").sql
    completed_sql = store.list_completed_by_task(
        task_name="tasks.sync",
        since=datetime.now(UTC).isoformat(),
    ).build(dialect="sqlite").sql
    cleanup_sql = store.cleanup_terminal(before=datetime.now(UTC).isoformat()).build(dialect="sqlite").sql
    ddl_sql = "\n".join(store.create_statements())

    assert '"task_id"' in insert_sql
    assert '"function"' in insert_sql
    assert '"task_name"' not in insert_sql
    assert '"adopter_jobs"."task_id" AS "id"' in select_sql
    assert '"adopter_jobs"."function" AS "task_name"' in select_sql
    assert '"adopter_jobs"."run_after" <= :now' in pending_sql
    assert '"lane" = ' in pending_sql
    assert "priority DESC" not in pending_sql
    assert '"adopter_jobs"."importance" DESC' in pending_sql
    assert '"state" = ' in claim_sql
    assert '"run_after" <= :due_at' in claim_sql
    assert '"outcome" = ' in complete_sql
    assert '"attempt_count" = "attempt_count" + 1' in stale_sql
    assert '"heartbeat" < :cutoff' in stale_sql
    assert "\"adopter_jobs\".\"state\" IN ('pending', 'scheduled', 'running')" in external_sql
    assert 'NOT "adopter_jobs"."external_ref" IS NULL' in external_sql
    assert '"adopter_jobs"."finished" >= ' in completed_sql
    assert '"finished" < :terminal_before' in cleanup_sql
    assert 'NOT "finished" IS NULL' in cleanup_sql
    assert '"function" TEXT' in ddl_sql
    assert "task_name TEXT" not in ddl_sql


async def test_column_map_operates_against_adopter_owned_sqlite_table(
    tmp_path: "Path",
    sqlite_config_factory: Callable[["Path"], AiosqliteConfig],
) -> None:
    """The backend can run end-to-end against a table that uses adopter names."""
    db_path = tmp_path / "adopter.db"
    _create_adopter_sqlite_schema(db_path)
    backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(
            sqlspec_config=sqlite_config_factory(db_path),
            table_name="adopter_jobs",
            column_map=ADOPTER_COLUMN_MAP,
            manage_schema=False,
        )
    )

    await backend.open()
    try:
        record = await backend.enqueue(
            "tasks.adopter",
            args=("account-1",),
            kwargs={"attempt": 1},
            queue="critical",
            priority=10,
            key="unique-1",
            execution_backend="cloudrun",
            execution_profile="batch",
            metadata={"tenant": "acme"},
        )
        fetched = await backend.get_task(record.id)
        keyed = await backend.get_task_by_key("unique-1")
        pending = await backend.list_pending(queue="critical", execution_backend="cloudrun")
        claimed = await backend.claim_task(record.id)
        assert claimed is not None

        referenced = await backend.set_execution_ref(
            claimed.id,
            "cloudrun",
            "job-123",
            execution_profile="batch",
        )
        running_external = await backend.list_running_external(limit=10)
        completed = await backend.complete_task(claimed.id, result={"ok": True})
        completed_by_task = await backend.list_completed_by_task(
            "tasks.adopter",
            since=datetime.now(UTC) - timedelta(minutes=1),
        )
    finally:
        await backend.close()

    assert fetched is not None
    assert fetched.args == ("account-1",)
    assert fetched.kwargs == {"attempt": 1}
    assert fetched.metadata == {"tenant": "acme"}
    assert keyed is not None
    assert keyed.id == record.id
    assert [item.id for item in pending] == [record.id]
    assert referenced is not None
    assert referenced.execution_ref == "job-123"
    assert [item.id for item in running_external] == [record.id]
    assert completed is not None
    assert completed.result == {"ok": True}
    assert [item.id for item in completed_by_task] == [record.id]

    with sqlite3.connect(db_path) as connection:
        row = connection.execute(
            'SELECT "function", "data", "outcome", "meta", "external_ref" FROM adopter_jobs'
        ).fetchone()
        tables = {table[0] for table in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}

    assert row == ("tasks.adopter", '{"attempt":1}', '{"ok":true}', '{"tenant":"acme"}', "job-123")
    assert tables == {"adopter_jobs"}


async def test_manage_schema_false_emits_no_schema_ddl(
    tmp_path: "Path",
    sqlite_config_factory: Callable[["Path"], AiosqliteConfig],
) -> None:
    """Schema creation, drop, migrations, and open() stay hands-off when opted out."""
    db_path = tmp_path / "no-schema.db"
    config = sqlite_config_factory(db_path)
    backend = SQLSpecQueueBackend(backend_config=SQLSpecBackendConfig(sqlspec_config=config, manage_schema=False))
    migration = importlib.import_module("litestar_queues.backends.sqlspec.migrations.0001_create_queue_tasks")
    context = SimpleNamespace(config=config)
    setattr(config, "manage_schema", False)

    store = create_queue_store(config, manage_schema=False)
    await backend.open()
    await backend.close()

    assert store.create_statements() == []
    assert store.drop_statements() == []
    assert await migration.up(context) == []
    assert await migration.down(context) == []
    with sqlite3.connect(db_path) as connection:
        tables = {table[0] for table in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
    assert tables == set()


def test_native_json_columns_bypass_text_serialization(
    tmp_path: "Path",
    sqlite_config_factory: Callable[["Path"], AiosqliteConfig],
) -> None:
    """Native JSON columns pass Python values to SQLSpec while text JSON remains encoded."""
    store = create_queue_store(
        sqlite_config_factory(tmp_path / "native-json.db"),
        native_json_columns=frozenset({"kwargs_json"}),
    )
    payload = {"nested": ["value"]}

    assert store.serialize_json_column("kwargs_json", payload) is payload
    assert store.serialize_json_column("args_json", payload) == '{"nested":["value"]}'


@pytest.mark.parametrize(
    ("config_factory", "match"),
    (
        (lambda: SQLSpecBackendConfig(column_map={"does_not_exist": "task"}), "Unknown canonical column"),
        (lambda: SQLSpecBackendConfig(column_map={"task_name": "drop table"}), "Invalid SQL identifier"),
        (
            lambda: SQLSpecBackendConfig(native_json_columns=frozenset({"task_name"})),
            "native_json_columns contains non-JSON",
        ),
    ),
)
def test_backend_config_validates_column_map_and_native_json_columns(
    config_factory: Callable[[], SQLSpecBackendConfig],
    match: str,
) -> None:
    """Configuration mistakes fail before runtime SQL execution."""
    with pytest.raises(QueueConfigurationError, match=match):
        config_factory()


def _create_adopter_sqlite_schema(path: "Path") -> None:
    columns = ADOPTER_COLUMN_MAP
    with sqlite3.connect(path) as connection:
        connection.execute(
            f"""
            CREATE TABLE adopter_jobs (
                "{columns['id']}" TEXT PRIMARY KEY,
                "{columns['task_name']}" TEXT NOT NULL,
                "{columns['args_json']}" TEXT NOT NULL,
                "{columns['kwargs_json']}" TEXT NOT NULL,
                "{columns['queue']}" TEXT NOT NULL,
                "{columns['execution_backend']}" TEXT NOT NULL,
                "{columns['execution_profile']}" TEXT,
                "{columns['execution_ref']}" TEXT,
                "{columns['status']}" TEXT NOT NULL,
                "{columns['priority']}" INTEGER NOT NULL,
                "{columns['max_retries']}" INTEGER NOT NULL,
                "{columns['retry_count']}" INTEGER NOT NULL,
                "{columns['scheduled_at']}" TEXT,
                "{columns['created_at']}" TEXT NOT NULL,
                "{columns['started_at']}" TEXT,
                "{columns['completed_at']}" TEXT,
                "{columns['heartbeat_at']}" TEXT,
                "{columns['result_json']}" TEXT NOT NULL,
                "{columns['error']}" TEXT,
                "{columns['task_key']}" TEXT UNIQUE,
                "{columns['metadata_json']}" TEXT NOT NULL
            )
            """
        )

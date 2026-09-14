"""PostgreSQL durability across async and sync Psycopg transaction modes.

Canonical and application config subclasses share commit and fencing contracts.
Direct fallback calls exercise explicit transactions otherwise bypassed by the
PostgreSQL RETURNING paths.
"""

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, cast
from uuid import UUID, uuid4

import pytest

pytest.importorskip("psycopg")
pytest.importorskip("sqlspec")

from sqlspec.adapters.psycopg import PsycopgSyncConfig

from litestar_queues import TaskRequest
from litestar_queues.backends.sqlspec import SQLSpecBackendConfig, SQLSpecQueueBackend
from tests.integration._backends import QUEUE_BACKENDS, FixtureCtx
from tests.integration._names import table_name_for_test

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

    from litestar_queues.models import QueuedTaskRecord
    from tests.integration._backends import PostgresService

pytestmark = pytest.mark.anyio

_CASES_BY_NAME = {case.name: case for case in QUEUE_BACKENDS}
_PSYCOPG_CASE_NAMES = (
    "postgres-psycopg",
    "postgres-psycopg-autocommit",
    "sync",
    "sync-autocommit",
    "subclass-sync",
    "subclass-sync-autocommit",
)


class ApplicationDatabase(PsycopgSyncConfig):
    """An adopter's arbitrarily named synchronous config."""


@pytest.fixture(params=_PSYCOPG_CASE_NAMES)
async def psycopg_backend(
    request: "pytest.FixtureRequest", postgres_service: "PostgresService", tmp_path: "Path"
) -> "AsyncIterator[SQLSpecQueueBackend]":
    """Yield a real Psycopg backend for each adapter and autocommit mode."""
    name = request.param
    table_name = table_name_for_test("lq_psycopg_hotpath", name, request.node.nodeid)
    if name in _CASES_BY_NAME:
        ctx = FixtureCtx(tmp_path=tmp_path, service=postgres_service, table_name=table_name)
        backend = cast("SQLSpecQueueBackend", await _CASES_BY_NAME[name].build(ctx))
    else:
        config_type = ApplicationDatabase if name.startswith("subclass") else PsycopgSyncConfig
        config = config_type(
            connection_config={
                "host": postgres_service.host,
                "port": postgres_service.port,
                "user": postgres_service.user,
                "password": postgres_service.password,
                "dbname": postgres_service.database,
                "autocommit": name.endswith("autocommit"),
                "min_size": 1,
                "max_size": 2,
            }
        )
        backend = SQLSpecQueueBackend(
            backend_config=SQLSpecBackendConfig(sqlspec_config=config, queue_table_name=table_name)
        )
    await backend.open()
    await backend.create_schema()
    try:
        yield backend
    finally:
        await backend.close()


async def test_enqueue_keyed_dedupes_active_and_replaces_terminal_key(psycopg_backend: "SQLSpecQueueBackend") -> "None":
    """``_enqueue_keyed``'s explicit ``driver.begin()``/``commit()`` must dedupe and replace correctly."""
    first = await psycopg_backend.enqueue("tasks.keyed", key="dedupe:1", kwargs={"v": 1})
    duplicate = await psycopg_backend.enqueue("tasks.keyed", key="dedupe:1", kwargs={"v": 2})

    assert duplicate.id == first.id
    assert duplicate.kwargs == {"v": 1}

    claimed = await psycopg_backend.claim_task(first.id)
    assert claimed is not None
    await psycopg_backend.complete_task(claimed.id, result={"ok": True})

    replacement = await psycopg_backend.enqueue("tasks.keyed", key="dedupe:1", kwargs={"v": 3})

    assert replacement.id != first.id
    refetched = await psycopg_backend.get_task_by_key("dedupe:1")
    assert refetched is not None
    assert refetched.id == replacement.id


async def test_enqueue_many_uses_native_bulk_ingest_and_preserves_key_semantics(
    psycopg_backend: "SQLSpecQueueBackend",
) -> "None":
    """Psycopg uses SQLSpec's native Arrow COPY tier while preserving queue semantics."""
    store = psycopg_backend._get_store()
    config = psycopg_backend._get_sqlspec_config()

    assert getattr(type(config), "supports_native_arrow_import", False) is True
    assert store.supports_native_bulk_ingest is True

    active = await psycopg_backend.enqueue("tasks.bulk", key="bulk:active", kwargs={"v": 1})
    terminal = await psycopg_backend.enqueue("tasks.bulk", key="bulk:terminal", kwargs={"v": 1})
    claimed_terminal = await psycopg_backend.claim_task(terminal.id)
    assert claimed_terminal is not None
    await psycopg_backend.complete_task(claimed_terminal.id, result={"ok": True})

    records = await psycopg_backend.enqueue_many([
        TaskRequest(task_name="tasks.bulk", key="bulk:active", kwargs={"v": 2}),
        TaskRequest(task_name="tasks.bulk", key="bulk:terminal", kwargs={"v": 2}),
        TaskRequest(task_name="tasks.bulk", kwargs={"v": 3}),
    ])

    assert records[0].id == active.id
    assert records[0].kwargs == {"v": 1}  # active key: existing row returned as-is
    assert records[1].id != terminal.id  # terminal key: replaced with a fresh row
    assert records[1].kwargs == {"v": 2}
    assert records[2].kwargs == {"v": 3}

    stats = await psycopg_backend.get_statistics()
    assert stats.total == 4  # active + original terminal + its replacement + the fresh row


async def test_claim_task_and_claim_next_skip_locked_commit_under_the_configured_connection_mode(
    psycopg_backend: "SQLSpecQueueBackend",
) -> "None":
    """``claim_task`` and the SKIP LOCKED ``claim_next`` path both use explicit ``driver.begin()``."""
    record = await psycopg_backend.enqueue("tasks.claim")
    claimed = await psycopg_backend.claim_task(record.id)
    assert claimed is not None
    assert claimed.status == "running"

    second = await psycopg_backend.enqueue("tasks.claim.next")
    store = psycopg_backend._get_store()
    assert store.supports_skip_locked is True  # resolved from the connected PostgreSQL server

    claimed_next = await psycopg_backend.claim_next()
    assert claimed_next is not None
    assert claimed_next.id == second.id
    assert claimed_next.status == "running"


async def test_claim_next_optimistic_direct_invocation_commits_under_the_configured_connection_mode(
    psycopg_backend: "SQLSpecQueueBackend",
) -> "None":
    """The CAS-loop fallback is unreachable via ``claim_next()`` on Postgres (SKIP LOCKED always wins).

    Call it directly so its own ``driver.begin()``/``commit()`` cycle -- shared with every other
    sync-driver adapter that lacks SKIP LOCKED -- is still certified against a real psycopg connection.
    """
    record = await psycopg_backend.enqueue("tasks.optimistic")
    store = psycopg_backend._get_store()

    claimed = await psycopg_backend._claim_next_optimistic(store, queue=None, execution_backend=None)

    assert claimed is not None
    assert claimed.id == record.id
    assert claimed.status == "running"


async def test_complete_and_fail_task_without_returning_direct_invocation_commit_under_the_configured_connection_mode(
    psycopg_backend: "SQLSpecQueueBackend",
) -> "None":
    """``_complete_task_without_returning``/``_fail_task_without_returning`` are unreachable via the public API on Postgres

    (``supports_dml_returning`` routes ``complete_task``/``fail_task`` to the RETURNING fast path
    instead). Call them directly so their ``driver.begin()``/``commit()`` transactions are still
    certified against a real psycopg connection.
    """
    completed_record = await psycopg_backend.enqueue("tasks.without_returning.complete")
    claimed_complete = await psycopg_backend.claim_task(completed_record.id)
    assert claimed_complete is not None

    completed = await psycopg_backend._complete_task_without_returning(claimed_complete.id, result={"ok": True})

    assert completed is not None
    assert completed.status == "completed"
    stored_complete = await psycopg_backend.get_task(completed_record.id)
    assert stored_complete is not None
    assert stored_complete.status == "completed"

    failed_record = await psycopg_backend.enqueue("tasks.without_returning.fail")
    claimed_fail = await psycopg_backend.claim_task(failed_record.id)
    assert claimed_fail is not None

    failed = await psycopg_backend._fail_task_without_returning(claimed_fail.id, "boom", retry=False)

    assert failed is not None
    assert failed.status == "failed"
    stored_fail = await psycopg_backend.get_task(failed_record.id)
    assert stored_fail is not None
    assert stored_fail.status == "failed"
    assert stored_fail.error is not None


async def test_explicit_transaction_restores_pooled_connection_autocommit(
    postgres_service: "PostgresService", tmp_path: "Path", request: "pytest.FixtureRequest"
) -> "None":
    """SQLSpec restores a pooled psycopg connection's original autocommit setting."""
    from sqlspec.adapters.psycopg import PsycopgAsyncConfig

    from litestar_queues.backends.sqlspec import SQLSpecBackendConfig, SQLSpecQueueBackend

    table_name = table_name_for_test("lq_psycopg_restore", "postgres-psycopg-autocommit", request.node.nodeid)
    config = PsycopgAsyncConfig(
        connection_config={
            "host": postgres_service.host,
            "port": postgres_service.port,
            "user": postgres_service.user,
            "password": postgres_service.password,
            "dbname": postgres_service.database,
            "autocommit": True,
            "min_size": 1,
            "max_size": 1,
        }
    )
    backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(sqlspec_config=config, queue_table_name=table_name)
    )
    await backend.open()
    await backend.create_schema()
    try:
        pool = config.connection_instance
        assert pool is not None
        async with pool.connection() as conn:
            assert conn.autocommit is True

        record = await backend.enqueue("tasks.autocommit.transaction")
        claimed = await backend.claim_task(record.id)
        assert claimed is not None
        await backend.complete_task(claimed.id, result={"ok": True})

        async with pool.connection() as conn:
            assert conn.autocommit is True

        fast_path_record = await backend.enqueue("tasks.autocommit.fast_path")
        reread = await backend.get_task(fast_path_record.id)
        assert reread is not None
        assert reread.status == "pending"
    finally:
        await backend.close()


async def test_returning_enqueue_is_visible_before_wakeup(
    psycopg_backend: "SQLSpecQueueBackend", monkeypatch: "pytest.MonkeyPatch"
) -> "None":
    """Publication only observes a persisted enqueue, including sync sessions."""
    observed: list[UUID] = []

    async def notify(backend: "SQLSpecQueueBackend", record: "QueuedTaskRecord") -> None:
        stored = await backend.get_task(record.id)
        assert stored is not None
        assert stored.status == "pending"
        observed.append(stored.id)

    monkeypatch.setattr(type(psycopg_backend), "notify_new_task", notify)
    record = await psycopg_backend.enqueue("tasks.durable")
    assert observed == [record.id]
    stored = await psycopg_backend.get_task(record.id)
    assert stored is not None
    claimed = await psycopg_backend.claim_next()
    assert claimed is not None and claimed.id == record.id


@pytest.mark.parametrize("with_expired", [False, True])
async def test_returning_batch_claim_persists_filtered_fenced_outcomes(
    psycopg_backend: "SQLSpecQueueBackend", with_expired: bool
) -> None:
    due = await psycopg_backend.enqueue("due", queue="selected")
    capped = await psycopg_backend.enqueue("capped", queue="selected")
    other = await psycopg_backend.enqueue("other", queue="excluded")
    external = await psycopg_backend.enqueue("external", queue="selected", execution_backend="cloudtasks")
    expired = await psycopg_backend.enqueue(
        "expired", queue="selected", expires_at=datetime.now(timezone.utc) - timedelta(seconds=1)
    )
    if with_expired:
        claimed, expired_records = await psycopg_backend.claim_many_with_expired(
            limit=10, queues=("selected",), execution_backend="local", queue_limits={"selected": 1}
        )
        assert [record.id for record in expired_records] == [expired.id]
        persisted_expired = await psycopg_backend.get_task(expired.id)
        assert persisted_expired is not None and persisted_expired.status == "expired"
    else:
        claimed = await psycopg_backend.claim_many(
            limit=10, queues=("selected",), execution_backend="local", queue_limits={"selected": 1}
        )
    assert [record.id for record in claimed] == [due.id]
    persisted = await psycopg_backend.get_task(due.id)
    assert persisted is not None and persisted.status == "running" and persisted.retry_count == 0
    for untouched in (capped, other, external):
        stored = await psycopg_backend.get_task(untouched.id)
        assert stored is not None and stored.status == "pending"
    assert await psycopg_backend.claim_many(limit=1, queues=("empty",)) == []
    assert await psycopg_backend.claim_many_with_expired(limit=0) == ([], [])


async def test_returning_complete_retry_and_terminal_fail_persist_with_fences(
    psycopg_backend: "SQLSpecQueueBackend",
) -> None:
    record = await psycopg_backend.enqueue("retry", max_retries=1)
    assert await psycopg_backend.claim_task(record.id) is not None
    assert await psycopg_backend.fail_task(record.id, "stale", expected_retry_count=9) is None
    unchanged = await psycopg_backend.get_task(record.id)
    assert unchanged is not None and unchanged.status == "running" and unchanged.retry_count == 0
    retried = await psycopg_backend.fail_task(record.id, "retry", expected_retry_count=0)
    assert retried is not None and retried.retry_count == 1
    persisted_retry = await psycopg_backend.get_task(record.id)
    assert persisted_retry is not None and persisted_retry.status == retried.status and persisted_retry.retry_count == 1
    assert await psycopg_backend.claim_task(record.id, expected_retry_count=1) is not None
    assert await psycopg_backend.complete_task(record.id, result="stale", expected_retry_count=0) is None
    completed = await psycopg_backend.complete_task(record.id, result={"ok": True}, expected_retry_count=1)
    assert completed is not None and completed.status == "completed"
    persisted_complete = await psycopg_backend.get_task(record.id)
    assert persisted_complete is not None and persisted_complete.result == {"ok": True}
    terminal = await psycopg_backend.enqueue("terminal")
    assert await psycopg_backend.claim_task(terminal.id) is not None
    failed = await psycopg_backend.fail_task(terminal.id, "terminal", retry=False, expected_retry_count=0)
    assert failed is not None and failed.status == "failed"
    persisted_failure = await psycopg_backend.get_task(terminal.id)
    assert persisted_failure is not None and persisted_failure.status == "failed"


@pytest.mark.parametrize("psycopg_backend", ["sync", "sync-autocommit"], indirect=True)
@pytest.mark.parametrize("operation", ["enqueue", "retry"])
async def test_failed_returning_write_never_publishes_success(
    psycopg_backend: "SQLSpecQueueBackend", monkeypatch: "pytest.MonkeyPatch", operation: str
) -> None:
    """Uncommitted writes roll back; failing autocommit SQL never publishes."""
    from sqlspec.exceptions import SQLSpecError

    from litestar_queues.backends.sqlspec.backend import _ManagedAsyncDriver

    record_id = uuid4()
    if operation == "retry":
        record = await psycopg_backend.enqueue("retry failure", max_retries=1)
        record_id = record.id
        assert await psycopg_backend.claim_task(record_id) is not None
    notifications: list[UUID] = []

    async def notify(backend: "SQLSpecQueueBackend", record: "QueuedTaskRecord") -> None:
        notifications.append(record.id)

    config = cast("Any", psycopg_backend._get_sqlspec_config())
    autocommit = bool(config.connection_config["autocommit"])
    failure = RuntimeError("before commit")

    async def fail_commit(driver: _ManagedAsyncDriver) -> None:
        raise failure

    method = "execute" if operation == "enqueue" else "select"
    original = getattr(_ManagedAsyncDriver, method)

    async def fail_sql(driver: _ManagedAsyncDriver, *args: Any, **kwargs: Any) -> Any:
        return await original(driver, "INVALID SQL FOR DURABILITY TEST")

    with monkeypatch.context() as patch:
        patch.setattr(type(psycopg_backend), "notify_new_task", notify)
        if autocommit:
            patch.setattr(_ManagedAsyncDriver, method, fail_sql)
        else:
            patch.setattr(_ManagedAsyncDriver, "commit", fail_commit)
        with pytest.raises(SQLSpecError if autocommit else RuntimeError) as caught:
            if operation == "enqueue":
                await psycopg_backend.enqueue("failed enqueue", id=record_id)
            else:
                await psycopg_backend.fail_task(record_id, "retry", expected_retry_count=0)
        if not autocommit:
            assert caught.value is failure
        assert notifications == []

    stored = await psycopg_backend.get_task(record_id)
    if operation == "enqueue":
        assert stored is None
    else:
        assert stored is not None and stored.status == "running" and stored.retry_count == 0
    recovered = await psycopg_backend.enqueue("pool recovered")
    assert await psycopg_backend.get_task(recovered.id) is not None

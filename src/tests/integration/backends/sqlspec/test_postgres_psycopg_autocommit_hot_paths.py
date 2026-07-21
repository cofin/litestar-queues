"""Certification of the postgres-psycopg autocommit connection variant.

Beads litestar-queues-dh8.6: autocommit removes psycopg's implicit
per-statement ``BEGIN``/``COMMIT`` for the single-round-trip RETURNING fast
paths (plain ``enqueue``, ``complete_task``, ``fail_task``). A prior 50-job
roundtrip probe proved those fast paths correct but did not exercise the
explicit ``driver.begin()``/``driver.commit()`` call sites in
``SQLSpecQueueBackend``: keyed-enqueue dedupe (``_enqueue_keyed``), the
``enqueue_many`` bulk insert, and the claim/complete/fail transactional
fallbacks (``claim_task``, ``_claim_next_optimistic``,
``_claim_next_skip_locked``, ``_complete_task_legacy``, ``_fail_task_legacy``).
This module exercises every one of those against the real Postgres container
under both the plain and the autocommit psycopg configs so the two variants
can be compared directly.

``_complete_task_legacy``/``_fail_task_legacy`` are never reached through the
public dispatch on a Postgres-family adapter --
``PostgresQueueStore.supports_dml_returning`` is ``True``, so
``complete_task``/``fail_task`` always take the RETURNING fast path instead.
They are invoked directly here so the transaction plumbing itself is still
certified under autocommit even though production traffic on this adapter
never reaches it.
"""

from typing import TYPE_CHECKING, cast

import pytest

pytest.importorskip("psycopg")
pytest.importorskip("sqlspec")

from litestar_queues import EnqueueSpec
from tests.integration._backends import QUEUE_BACKENDS, FixtureCtx
from tests.integration._names import table_name_for_test

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

    from litestar_queues.backends.sqlspec import SQLSpecQueueBackend
    from tests.integration._backends import PostgresService

pytestmark = pytest.mark.anyio

_CASES_BY_NAME = {case.name: case for case in QUEUE_BACKENDS}
_PSYCOPG_CASE_NAMES = ("postgres-psycopg", "postgres-psycopg-autocommit")


@pytest.fixture(params=_PSYCOPG_CASE_NAMES)
async def psycopg_backend(
    request: "pytest.FixtureRequest", postgres_service: "PostgresService", tmp_path: "Path"
) -> "AsyncIterator[SQLSpecQueueBackend]":
    """Yield an opened backend for one of the two psycopg registry cases."""
    case = _CASES_BY_NAME[request.param]
    table_name = table_name_for_test("lq_psycopg_hotpath", case.name, request.node.nodeid)
    ctx = FixtureCtx(tmp_path=tmp_path, service=postgres_service, table_name=table_name)
    backend = cast("SQLSpecQueueBackend", await case.build(ctx))
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


async def test_enqueue_many_bulk_insert_dedupes_active_and_replaces_terminal_keys(
    psycopg_backend: "SQLSpecQueueBackend", monkeypatch: "pytest.MonkeyPatch"
) -> "None":
    """``enqueue_many``'s explicit ``driver.begin()`` bulk insert must dedupe/replace keys.

    Forces the universal ``execute_many`` bulk tier. psycopg's native
    ``load_from_records`` Arrow-COPY tier cannot adapt this store's native JSON
    columns (``kwargs_json``/``args_json``/``metadata_json``/``result_json`` are
    passed as raw Python dicts for normal parameterized INSERTs, which psycopg
    adapts to ``jsonb`` automatically) -- the COPY writer receives the same raw
    dicts and rejects them with ``cannot adapt type 'dict' using placeholder
    '%t'``. That failure reproduces identically with and without
    ``autocommit``, so it is an unrelated pre-existing SQLSpec/psycopg
    bulk-ingest gap, not something this Bead's autocommit change causes; this
    test routes around it to certify what it actually owns, the transaction
    envelope around the bulk insert.
    """
    store = psycopg_backend._get_store()
    monkeypatch.setattr(type(store), "supports_native_bulk_ingest", property(lambda _store: False))

    active = await psycopg_backend.enqueue("tasks.bulk", key="bulk:active", kwargs={"v": 1})
    terminal = await psycopg_backend.enqueue("tasks.bulk", key="bulk:terminal", kwargs={"v": 1})
    claimed_terminal = await psycopg_backend.claim_task(terminal.id)
    assert claimed_terminal is not None
    await psycopg_backend.complete_task(claimed_terminal.id, result={"ok": True})

    records = await psycopg_backend.enqueue_many([
        EnqueueSpec(task_name="tasks.bulk", key="bulk:active", kwargs={"v": 2}),
        EnqueueSpec(task_name="tasks.bulk", key="bulk:terminal", kwargs={"v": 2}),
        EnqueueSpec(task_name="tasks.bulk", kwargs={"v": 3}),
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
    assert store.supports_skip_locked is True  # postgres dialect always advertises SKIP LOCKED

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


async def test_complete_and_fail_task_legacy_direct_invocation_commit_under_the_configured_connection_mode(
    psycopg_backend: "SQLSpecQueueBackend",
) -> "None":
    """``_complete_task_legacy``/``_fail_task_legacy`` are unreachable via the public API on Postgres

    (``supports_dml_returning`` routes ``complete_task``/``fail_task`` to the RETURNING fast path
    instead). Call them directly so their ``driver.begin()``/``commit()`` transactions are still
    certified against a real psycopg connection.
    """
    completed_record = await psycopg_backend.enqueue("tasks.legacy.complete")
    claimed_complete = await psycopg_backend.claim_task(completed_record.id)
    assert claimed_complete is not None

    completed = await psycopg_backend._complete_task_legacy(claimed_complete.id, result={"ok": True})

    assert completed is not None
    assert completed.status == "completed"
    stored_complete = await psycopg_backend.get_task(completed_record.id)
    assert stored_complete is not None
    assert stored_complete.status == "completed"

    failed_record = await psycopg_backend.enqueue("tasks.legacy.fail")
    claimed_fail = await psycopg_backend.claim_task(failed_record.id)
    assert claimed_fail is not None

    failed = await psycopg_backend._fail_task_legacy(claimed_fail.id, "boom", retry=False)

    assert failed is not None
    assert failed.status == "failed"
    stored_fail = await psycopg_backend.get_task(failed_record.id)
    assert stored_fail is not None
    assert stored_fail.status == "failed"
    assert stored_fail.error is not None


async def test_fast_path_enqueue_still_commits_after_claim_task_taints_pooled_autocommit_connection(
    postgres_service: "PostgresService", tmp_path: "Path", request: "pytest.FixtureRequest"
) -> "None":
    """Pin the correctness half of a real SQLSpec/psycopg quirk found while certifying autocommit.

    SQLSpec's psycopg driver ``begin()`` flips ``connection.autocommit`` off for the duration of an
    explicit transaction (see ``sqlspec/adapters/psycopg/driver.py``) but never restores it to
    ``True`` afterward -- that only happens once, in ``_configure_async_connection``, when a
    physical connection is first created by the pool. So once any ``driver.begin()``-using call
    (``claim_task``, keyed enqueue, ``enqueue_many``, the SKIP LOCKED claim) lands on a pooled
    connection, later fast-path calls that reuse that *same* physical connection revert to an
    implicit ``BEGIN`` plus a real trailing ``COMMIT`` sent by SQLSpec's ``pool.connection()``
    wrapper on session release -- i.e. the wire-traffic reduction this Bead measured erodes for
    that connection, but every write still commits (SQLSpec's session wrapper always resolves the
    connection's transaction on checkout release, autocommit or not). This test pins that
    correctness guarantee: reads never observe a lost or stale write after the taint. A
    ``min_size=1, max_size=1`` pool forces exactly one physical connection so the taint is
    deterministic instead of depending on which pool member a later call happens to draw.
    """
    from sqlspec.adapters.psycopg import PsycopgAsyncConfig

    from litestar_queues.backends.sqlspec import SQLSpecBackendConfig, SQLSpecQueueBackend

    table_name = table_name_for_test("lq_psycopg_taint", "postgres-psycopg-autocommit", request.node.nodeid)
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
    backend = SQLSpecQueueBackend(backend_config=SQLSpecBackendConfig(config=config, queue_table_name=table_name))
    await backend.open()
    await backend.create_schema()
    try:
        pool = config.connection_instance
        assert pool is not None
        async with pool.connection() as conn:
            assert conn.autocommit is True

        # claim_task's driver.begin() taints the sole pooled connection.
        record = await backend.enqueue("tasks.taint.trigger")
        claimed = await backend.claim_task(record.id)
        assert claimed is not None
        await backend.complete_task(claimed.id, result={"ok": True})

        async with pool.connection() as conn:
            assert conn.autocommit is False  # pins the known SQLSpec quirk described above

        # A fast-path enqueue reusing the now-tainted connection must still commit.
        fast_path_record = await backend.enqueue("tasks.taint.fast_path")
        reread = await backend.get_task(fast_path_record.id)
        assert reread is not None
        assert reread.status == "pending"
    finally:
        await backend.close()

"""Fixtures and parametrize hook for Advanced Alchemy backend tests.

Provides the ``advanced_alchemy_backend`` async fixture that yields an
opened ``AdvancedAlchemyQueueBackend`` parametrized over ``AA_ENGINES``.
For service-backed engines, drops the queue tables on teardown so the
shared Docker DB stays isolated between tests.
"""

from contextlib import suppress
from typing import TYPE_CHECKING

import pytest

from tests.integration._backends import FixtureCtx
from tests.integration.backends.advanced_alchemy._aa_engines import AA_ENGINES, AAEngineCase

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

    from litestar_queues.backends.advanced_alchemy import AdvancedAlchemyQueueBackend


@pytest.fixture
async def advanced_alchemy_backend(
    request: pytest.FixtureRequest,
    tmp_path: "Path",
) -> "AsyncIterator[AdvancedAlchemyQueueBackend]":
    """Yield an opened Advanced Alchemy queue backend parametrized over AA_ENGINES.

    For service-backed engines (Postgres/MySQL/Oracle), the queue + alembic
    bookkeeping tables are dropped on teardown to keep the shared Docker DB
    clean between tests. In-process (aiosqlite) gets a unique tmp_path DB
    file per test so no extra cleanup is required.
    """
    from litestar_queues.backends.advanced_alchemy import AdvancedAlchemyQueueBackend

    case: AAEngineCase = request.param
    for extra in case.extras:
        pytest.importorskip(extra)

    service = None
    if case.service_attr is not None:
        try:
            service = request.getfixturevalue(case.service_attr)
        except pytest.FixtureLookupError:
            pytest.skip(f"{case.name} requires fixture {case.service_attr}")
        if service is None:
            pytest.skip(f"{case.name} requires {case.service_attr} (Docker unavailable)")

    ctx = FixtureCtx(
        tmp_path=tmp_path,
        postgres_service=service if case.service_attr == "postgres_service" else None,
        mysql_service=service if case.service_attr == "mysql_service" else None,
        oracle_service=service if case.service_attr == "oracle_service" else None,
    )
    config = case.build_config(ctx)
    backend = AdvancedAlchemyQueueBackend(sqlalchemy_config=config, create_schema=True)
    await backend.open()
    try:
        yield backend
    finally:
        if case.service_attr is not None:
            with suppress(Exception):
                await _drop_queue_tables(backend)
        await backend.close()


async def _drop_queue_tables(backend: "AdvancedAlchemyQueueBackend") -> None:
    """Drop the queue + alembic bookkeeping tables for service-backed engines.

    Uses the dialect-aware ``Table.drop()`` path for the queue table so
    identifier quoting matches the engine (backticks on MySQL, double-
    quotes on Postgres, uppercase on Oracle). Bookkeeping tables fall
    back to unquoted DDL so they work across every dialect.
    """
    from typing import Any, cast

    from sqlalchemy import text

    from litestar_queues.backends.advanced_alchemy.models import QueueTaskModel

    sqlalchemy_config = backend._sqlalchemy_config
    if sqlalchemy_config is None:
        return
    engine = sqlalchemy_config.get_engine()
    async with engine.begin() as connection:
        with suppress(Exception):
            await connection.run_sync(cast("Any", QueueTaskModel.__table__).drop, checkfirst=True)
        for ddl in ("DROP TABLE IF EXISTS ddl_migrations", "DROP TABLE IF EXISTS alembic_version"):
            with suppress(Exception):
                await connection.execute(text(ddl))


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    """Parametrize ``advanced_alchemy_backend`` consumers over AA_ENGINES.

    Cases tagged with ``xfail-upstream`` are wrapped in
    ``pytest.param(..., marks=xfail)`` so the suite reports a clean signal
    without failing CI. When the upstream fix lands, drop the capability
    from the AAEngineCase to flip the case back to a hard-pass requirement.
    """
    if "advanced_alchemy_backend" in metafunc.fixturenames:
        params = []
        for case in AA_ENGINES:
            marks: list[pytest.MarkDecorator] = []
            if "xfail-upstream" in case.capabilities:
                marks.append(
                    pytest.mark.xfail(
                        reason=f"{case.name}: upstream Advanced Alchemy/adapter blocker (see litestar-queues-27b)",
                        strict=False,
                    )
                )
            params.append(pytest.param(case, marks=marks, id=case.name))
        metafunc.parametrize("advanced_alchemy_backend", params, indirect=True)

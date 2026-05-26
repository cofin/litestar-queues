"""Integration-tier pytest fixtures and pytest-databases plugin registration."""

from contextlib import suppress
from typing import TYPE_CHECKING

import pytest

from tests.integration._backends import QUEUE_BACKENDS, BackendCase, FixtureCtx

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

    from litestar_queues.backends import BaseQueueBackend


@pytest.fixture
async def queue_backend(request: pytest.FixtureRequest, tmp_path: "Path") -> "AsyncIterator[BaseQueueBackend]":
    """Yield an opened queue backend parametrized over QUEUE_BACKENDS.

    For service-backed adapters (Postgres, MySQL, Oracle), tests share the same
    Docker database across the run; we drop the queue table on teardown to
    prevent cross-test data leakage. In-process adapters get a unique tmp_path
    DB file per test so no extra cleanup is required.
    """
    case: BackendCase = request.param
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

    # Service-backed adapters share the same Docker database across the
    # session. Give each parametrized case its own queue table so adapters
    # cannot collide via the shared default ``litestar_queue_tasks`` name.
    table_name = f"litestar_queue_tasks_{case.name.replace('-', '_')}" if case.service_attr is not None else None
    ctx = FixtureCtx(tmp_path=tmp_path, service=service, table_name=table_name)

    backend = await case.build(ctx)
    await backend.open()
    try:
        yield backend
    finally:
        if case.service_attr is not None:
            with suppress(Exception):
                await _drop_queue_tables(backend)
        await backend.close()


async def _drop_queue_tables(backend: "BaseQueueBackend") -> None:
    """Drop the queue + events tables for service-backed SQLSpec adapters."""
    sqlspec_config = getattr(backend, "_sqlspec_config", None)
    sqlspec_manager = getattr(backend, "_sqlspec", None)
    if sqlspec_config is None or sqlspec_manager is None:
        return
    table_name = getattr(backend, "_table_name", None) or "litestar_queue_tasks"
    store = getattr(backend, "_store", None)
    from litestar_queues.backends.sqlspec.backend import _bridge_session

    async with _bridge_session(sqlspec_manager, sqlspec_config) as driver:
        for ddl in (
            *(store.drop_statements() if store is not None else ()),
            f'DROP TABLE IF EXISTS "{table_name}"',
            'DROP TABLE IF EXISTS "ddl_migrations"',
            'DROP TABLE IF EXISTS "sqlspec_async_events"',
        ):
            with suppress(Exception):
                await driver.execute_script(ddl)


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    """Parametrize any test consuming the `queue_backend` fixture across QUEUE_BACKENDS.

    Cases tagged with ``skip-upstream`` are known to hang or fail before the
    contract body can run. Cases tagged with ``xfail-upstream`` still run but
    report a clean signal without failing CI. See
    ``tests/integration/_backends.py`` for the rationale.
    """
    if "queue_backend" in metafunc.fixturenames:
        params = []
        for case in QUEUE_BACKENDS:
            marks: list[pytest.MarkDecorator] = []
            if "skip-upstream" in case.capabilities:
                marks.append(
                    pytest.mark.skip(reason=f"{case.name}: upstream SQLSpec/adapter blocker (see litestar-queues-27b)")
                )
            elif "xfail-upstream" in case.capabilities:
                marks.append(
                    pytest.mark.xfail(
                        reason=f"{case.name}: upstream SQLSpec/adapter blocker (see litestar-queues-27b)", strict=False
                    )
                )
            params.append(pytest.param(case, marks=marks, id=case.name))
        metafunc.parametrize("queue_backend", params, indirect=True)

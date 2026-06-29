"""Fixtures and parametrize hook for Advanced Alchemy backend tests.

Provides the ``advanced_alchemy_backend`` async fixture that yields an
opened ``AdvancedAlchemyQueueBackend`` parametrized over ``AA_ENGINES``.
For service-backed engines, drops the queue table on teardown so the
shared Docker DB stays isolated between tests.
"""

from contextlib import suppress
from typing import TYPE_CHECKING, Protocol, cast

import pytest

from tests.integration._backends import FixtureCtx
from tests.integration._names import table_name_for_test
from tests.integration.backends.advanced_alchemy._aa_engines import AA_ENGINES, AAEngineCase

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

    from sqlalchemy import Table

    from litestar_queues.backends.advanced_alchemy import AdvancedAlchemyQueueBackend


class MappedQueueModel(Protocol):
    """Structural type for app-owned SQLAlchemy queue models."""

    __table__: "Table"


def _queue_model(table_name: "str") -> "type[object]":
    """Return the app-owned queue model used by integration tests."""
    from advanced_alchemy.base import UUIDAuditBase

    from litestar_queues.backends.advanced_alchemy import QueueTaskModelMixin

    suffix = table_name.rsplit("_", 1)[-1]
    return type(
        f"IntegrationQueueTask{suffix}",
        (UUIDAuditBase, QueueTaskModelMixin),
        {"__module__": __name__, "__tablename__": table_name},
    )


@pytest.fixture
async def advanced_alchemy_backend(
    request: "pytest.FixtureRequest", tmp_path: "Path"
) -> "AsyncIterator[AdvancedAlchemyQueueBackend]":
    """Yield an opened Advanced Alchemy queue backend parametrized over AA_ENGINES.

    For service-backed engines (Postgres/MySQL/Oracle), the queue table is
    dropped on teardown to keep the shared Docker DB clean between tests.
    In-process (aiosqlite) gets a unique tmp_path DB file per test so no extra
    cleanup is required.
    """
    from litestar_queues.backends.advanced_alchemy import AdvancedAlchemyBackendConfig, AdvancedAlchemyQueueBackend

    case: "AAEngineCase" = request.param
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

    ctx = FixtureCtx(tmp_path=tmp_path, service=service)
    config = case.build_config(ctx)
    table_name = table_name_for_test("aa_queue_task", case.name, request.node.nodeid)
    backend = AdvancedAlchemyQueueBackend(
        backend_config=AdvancedAlchemyBackendConfig(
            sqlalchemy_config=config, model_class=_queue_model(table_name), create_schema=True
        )
    )
    await backend.open()
    try:
        yield backend
    finally:
        if case.service_attr is not None:
            with suppress(Exception):
                await _drop_queue_tables(backend)
        await backend.close()


async def _drop_queue_tables(backend: "AdvancedAlchemyQueueBackend") -> "None":
    """Drop the queue table for service-backed engines.

    Uses the dialect-aware ``Table.drop()`` path for the queue table so
    identifier quoting matches the engine (backticks on MySQL, double-
    quotes on Postgres, uppercase on Oracle).
    """

    sqlalchemy_config = backend._sqlalchemy_config
    if sqlalchemy_config is not None:
        model_class = cast("MappedQueueModel", backend._model_class)
        engine = sqlalchemy_config.get_engine()
        async with engine.begin() as connection:
            with suppress(Exception):
                await connection.run_sync(model_class.__table__.drop, checkfirst=True)


def pytest_generate_tests(metafunc: "pytest.Metafunc") -> "None":
    """Parametrize ``advanced_alchemy_backend`` consumers over AA_ENGINES."""
    if "advanced_alchemy_backend" in metafunc.fixturenames:
        params = []
        for case in AA_ENGINES:
            marks: "list[pytest.MarkDecorator]" = []
            if case.service_attr is not None:
                marks.append(pytest.mark.xdist_group(case.name))
            params.append(pytest.param(case, marks=marks, id=case.name))
        metafunc.parametrize("advanced_alchemy_backend", params, indirect=True)

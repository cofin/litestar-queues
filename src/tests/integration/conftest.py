"""Integration-tier pytest fixtures and pytest-databases plugin registration."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from tests.integration._backends import QUEUE_BACKENDS, BackendCase, FixtureCtx

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

    from litestar_queues.backends import BaseQueueBackend

# pytest-databases auto-skips each plugin when Docker is unavailable.
pytest_plugins = [
    "pytest_databases.docker.postgres",
    "pytest_databases.docker.mysql",
    "pytest_databases.docker.oracle",
]


@pytest.fixture
async def queue_backend(
    request: pytest.FixtureRequest,
    tmp_path: "Path",
) -> "AsyncIterator[BaseQueueBackend]":
    """Yield an opened queue backend parametrized over QUEUE_BACKENDS."""
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

    ctx = FixtureCtx(
        tmp_path=tmp_path,
        postgres_service=service if case.service_attr == "postgres_service" else None,
        mysql_service=service if case.service_attr == "mysql_service" else None,
        mariadb_service=service if case.service_attr == "mariadb_service" else None,
        oracle_service=service if case.service_attr == "oracle_service" else None,
    )

    backend = await case.build(ctx)
    await backend.open()
    try:
        yield backend
    finally:
        await backend.close()


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    """Parametrize any test consuming the `queue_backend` fixture across QUEUE_BACKENDS.

    Cases tagged with ``xfail-upstream`` are wrapped in ``pytest.param(..., marks=xfail)``
    so the test suite reports a clean signal without failing CI. See
    ``tests/integration/_backends.py`` for the rationale.
    """
    if "queue_backend" in metafunc.fixturenames:
        params = []
        for case in QUEUE_BACKENDS:
            marks: list[pytest.MarkDecorator] = []
            if "xfail-upstream" in case.capabilities:
                marks.append(
                    pytest.mark.xfail(
                        reason=f"{case.name}: upstream SQLSpec/adapter blocker (see litestar-queues-27b)",
                        strict=False,
                    )
                )
            params.append(pytest.param(case, marks=marks, id=case.name))
        metafunc.parametrize("queue_backend", params, indirect=True)

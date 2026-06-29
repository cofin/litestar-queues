"""Integration-tier pytest fixtures and pytest-databases plugin registration."""

from typing import TYPE_CHECKING, cast

import pytest

from tests.integration._backends import QUEUE_BACKENDS, BackendCase, FixtureCtx
from tests.integration._names import table_name_for_test

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

    from litestar_queues.backends import BaseQueueBackend


@pytest.fixture
async def queue_backend(request: "pytest.FixtureRequest", tmp_path: "Path") -> "AsyncIterator[BaseQueueBackend]":
    """Yield an opened queue backend parametrized over QUEUE_BACKENDS.

    Service-backed adapters (Postgres, MySQL, Oracle) run against the
    pytest-databases service database and get a unique queue table per test.
    In-process adapters get a unique tmp_path DB file per test.
    """
    case: "BackendCase" = request.param
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
    # session. Give each parametrized test its own queue table so adapters and
    # test cases cannot collide via the shared default name.
    table_name = (
        table_name_for_test("litestar_queue_task", case.name, request.node.nodeid)
        if case.service_attr is not None
        else None
    )
    ctx = FixtureCtx(tmp_path=tmp_path, service=service, table_name=table_name)

    backend = await case.build(ctx)
    await backend.open()
    try:
        yield backend
    finally:
        await backend.close()


@pytest.fixture
def queue_backend_case(request: "pytest.FixtureRequest") -> "BackendCase":
    """Return the ``BackendCase`` backing the active ``queue_backend`` parametrization.

    Lets capability-sensitive tests (e.g. concurrency, which single-writer
    sync drivers cannot satisfy) introspect the case without re-parametrizing.
    """
    return cast("BackendCase", request.node.callspec.params["queue_backend"])


def pytest_generate_tests(metafunc: "pytest.Metafunc") -> "None":
    """Parametrize any test consuming the `queue_backend` fixture across QUEUE_BACKENDS.

    Service-backed cases are marked with an xdist group so tests sharing one
    Docker service run serially per adapter.
    """
    if "queue_backend" in metafunc.fixturenames:
        params = []
        for case in QUEUE_BACKENDS:
            marks: "list[pytest.MarkDecorator]" = []
            if case.service_attr is not None:
                marks.append(pytest.mark.xdist_group(case.name))
            params.append(pytest.param(case, marks=marks, id=case.name))
        metafunc.parametrize("queue_backend", params, indirect=True)

"""Shared fixtures and helpers for SQLSpec-focused integration tests.

These tests target SQLSpec-specific behaviour that does not parametrize cleanly
across every adapter in the registry (e.g., notification channels, heartbeat
pool isolation, Oracle DDL choices). The aiosqlite-pinned ``sqlspec_backend``
fixture defined here is the SQLSpec counterpart to the parametrized
``queue_backend`` fixture exposed from the parent ``conftest.py``.
"""

import asyncio
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, TypeAlias

import pytest

if TYPE_CHECKING:
    from pathlib import Path

pytest.importorskip("aiosqlite")
pytest.importorskip("sqlspec")

from sqlspec.adapters.aiosqlite import AiosqliteConfig

from litestar_queues.backends.sqlspec import SQLSpecBackendConfig, SQLSpecQueueBackend

SqliteConfigFactory: "TypeAlias" = Callable[["Path"], AiosqliteConfig]
EventPayload: "TypeAlias" = dict[str, object]
EventMetadata: "TypeAlias" = dict[str, object]


def _sqlite_config(path: "Path") -> "AiosqliteConfig":
    """Return an aiosqlite SQLSpec config pointing at ``path``."""
    return AiosqliteConfig(connection_config={"database": str(path)})


@pytest.fixture
def sqlite_config_factory() -> "SqliteConfigFactory":
    """Return the ``_sqlite_config`` helper as a fixture."""
    return _sqlite_config


@pytest.fixture
async def sqlspec_backend(tmp_path: "Path") -> "AsyncIterator[SQLSpecQueueBackend]":
    """Yield an opened aiosqlite-backed SQLSpec queue backend."""
    backend = SQLSpecQueueBackend(backend_config=SQLSpecBackendConfig(config=_sqlite_config(tmp_path / "queue.db")))
    await backend.open()
    await backend.create_schema()
    try:
        yield backend
    finally:
        await backend.close()


@pytest.fixture
async def duckdb_backend(tmp_path: "Path") -> "AsyncIterator[SQLSpecQueueBackend]":
    """Yield an opened DuckDB-backed SQLSpec queue backend."""
    pytest.importorskip("duckdb")
    from sqlspec.adapters.duckdb import DuckDBConfig

    backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(
            config=DuckDBConfig(connection_config={"database": str(tmp_path / "queue.duckdb")})
        )
    )
    await backend.open()
    await backend.create_schema()
    try:
        yield backend
    finally:
        await backend.close()


@dataclass(slots=True)
class StubEvent:
    """Test-double event passed by ``StubAsyncEventChannel``."""

    event_id: "str"
    payload: "EventPayload"
    metadata: "EventMetadata | None" = None


class StubAsyncEventChannel:
    """In-memory test double mirroring SQLSpec's ``AsyncEventChannel`` interface."""

    __slots__ = ("_backend_name", "_events", "acked", "published")

    def __init__(self, backend_name: "str" = "table_queue") -> "None":
        self._backend_name = backend_name
        self.acked: "list[str]" = []
        self.published: "list[tuple[str, EventPayload, EventMetadata | None]]" = []
        self._events: "asyncio.Queue[StubEvent]" = asyncio.Queue()

    async def publish(self, channel: "str", payload: "EventPayload", metadata: "EventMetadata | None" = None) -> "str":
        event_id = f"event-{len(self.published) + 1}"
        self.published.append((channel, payload, metadata))
        await self._events.put(StubEvent(event_id, payload, metadata))
        return event_id

    async def iter_events(self, channel: "str", *, poll_interval: "float | None" = None) -> "AsyncIterator[StubEvent]":
        while True:
            if poll_interval is None:
                event = await self._events.get()
            else:
                try:
                    event = await asyncio.wait_for(self._events.get(), timeout=poll_interval)
                except asyncio.TimeoutError:
                    continue
            if channel == self.published[-1][0]:
                yield event

    async def ack(self, event_id: "str") -> "None":
        self.acked.append(event_id)

    async def shutdown(self) -> "None":
        return None

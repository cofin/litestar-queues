"""Integration tests for SQLSpec streaming reads.

``iter_all`` streams the full table through ``select_stream`` instead of
materializing every row, and ``get_statistics`` consumes the same stream.
"""

from inspect import isasyncgen
from typing import TYPE_CHECKING

import pytest

from litestar_queues import EnqueueSpec
from litestar_queues.backends.sqlspec import SQLSpecQueueBackend
from litestar_queues.models import QueuedTaskRecord

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

pytestmark = pytest.mark.anyio


async def _drain(stream: "AsyncIterator[QueuedTaskRecord]") -> list[QueuedTaskRecord]:
    return [record async for record in stream]


async def test_iter_all_yields_every_record(sqlspec_backend: SQLSpecQueueBackend) -> None:
    enqueued = await sqlspec_backend.enqueue_many([EnqueueSpec(task_name=f"tasks.t{i}", args=(i,)) for i in range(25)])
    expected_ids = {record.id for record in enqueued}

    streamed = await _drain(sqlspec_backend.iter_all(chunk_size=4))

    assert len(streamed) == 25
    assert all(isinstance(record, QueuedTaskRecord) for record in streamed)
    assert {record.id for record in streamed} == expected_ids


async def test_iter_all_returns_async_generator(sqlspec_backend: SQLSpecQueueBackend) -> None:
    stream = sqlspec_backend.iter_all()
    assert isasyncgen(stream)
    await _drain(stream)


async def test_iter_all_empty_table(sqlspec_backend: SQLSpecQueueBackend) -> None:
    assert await _drain(sqlspec_backend.iter_all()) == []


async def test_get_statistics_counts_large_batch(sqlspec_backend: SQLSpecQueueBackend) -> None:
    await sqlspec_backend.enqueue_many([EnqueueSpec(task_name=f"tasks.t{i}") for i in range(30)])

    stats = await sqlspec_backend.get_statistics()

    assert stats.pending == 30
    assert stats.total == 30

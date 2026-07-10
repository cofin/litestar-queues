"""SQLSpec datetime round-trip regression tests."""

import os
import time
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

import pytest

from litestar_queues.backends.sqlspec import SQLSpecBackendConfig, SQLSpecQueueBackend
from litestar_queues.backends.sqlspec.stores.factory import create_queue_store

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.anyio


def test_duckdb_serializes_aware_datetimes_as_naive_utc_for_timestamp_columns() -> "None":
    pytest.importorskip("duckdb")
    from sqlspec.adapters.duckdb import DuckDBConfig

    backend = SQLSpecQueueBackend()
    backend._store = create_queue_store(DuckDBConfig(connection_config={"database": ":memory:"}))
    value = datetime(2026, 7, 2, 7, 0, tzinfo=timezone(timedelta(hours=-5)))

    serialized = backend._serialize_datetime(value)
    expected = datetime(2026, 7, 2, 12, 0, tzinfo=timezone.utc).replace(tzinfo=None)

    assert serialized == expected
    assert isinstance(serialized, datetime)
    assert serialized.tzinfo is None


async def test_duckdb_roundtrips_aware_utc_datetimes_on_non_utc_host(
    monkeypatch: "pytest.MonkeyPatch", tmp_path: "Path"
) -> "None":
    if not hasattr(time, "tzset"):
        pytest.skip("process timezone changes require time.tzset()")

    pytest.importorskip("duckdb")
    from sqlspec.adapters.duckdb import DuckDBConfig

    original_tz = os.environ.get("TZ")
    _set_process_timezone(monkeypatch, "America/Chicago")
    backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(
            config=DuckDBConfig(connection_config={"database": str(tmp_path / "queue.duckdb")})
        )
    )
    await backend.open()
    await backend.create_schema()
    try:
        scheduled_at = datetime(2026, 7, 2, 12, 0, tzinfo=timezone.utc)
        record = await backend.enqueue("tasks.duckdb-timezone", scheduled_at=scheduled_at)

        stored = await backend.get_task(record.id)

        assert stored is not None
        assert stored.scheduled_at == scheduled_at
    finally:
        await backend.close()
        _restore_process_timezone(monkeypatch, original_tz)


def _set_process_timezone(monkeypatch: "pytest.MonkeyPatch", value: "str") -> "None":
    monkeypatch.setenv("TZ", value)
    time.tzset()


def _restore_process_timezone(monkeypatch: "pytest.MonkeyPatch", value: "str | None") -> "None":
    if value is None:
        monkeypatch.delenv("TZ", raising=False)
    else:
        monkeypatch.setenv("TZ", value)
    time.tzset()

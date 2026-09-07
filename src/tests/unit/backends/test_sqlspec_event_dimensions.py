"""Package-owned scoping dimensions on the SQLSpec event-history table."""

import pytest

pytest.importorskip("sqlspec")
pytest.importorskip("aiosqlite")

from sqlspec.adapters.aiosqlite import AiosqliteConfig

from litestar_queues.backends.sqlspec.event_log import SQLSpecQueueEventLogStore, create_event_log_store
from litestar_queues.backends.sqlspec.schema import EVENT_HISTORY_COLUMNS

DIMENSIONS = ("scope", "scope_key", "actor", "entity")


def _store() -> "SQLSpecQueueEventLogStore":
    return create_event_log_store(
        AiosqliteConfig(connection_config={"database": ":memory:"}), queue_table_name="queue_task"
    )


def test_dimensions_are_package_owned_columns() -> "None":
    assert EVENT_HISTORY_COLUMNS[-4:] == DIMENSIONS
    assert len(EVENT_HISTORY_COLUMNS) == 25


def test_dimensions_appear_in_ddl_insert_and_indexes() -> "None":
    store = _store()
    statements = store.create_statements()
    template = store.insert_events_template()

    create_table = next(s for s in statements if s.startswith("CREATE TABLE"))
    assert all(name in create_table for name in DIMENSIONS)
    assert all(f":{name}" in template for name in DIMENSIONS)
    assert any("scope_key" in s and s.startswith("CREATE INDEX") for s in statements)
    assert any("entity" in s and s.startswith("CREATE INDEX") for s in statements)


@pytest.mark.anyio
@pytest.mark.parametrize("cancelled", [False, True])
@pytest.mark.parametrize("cleanup", ["raise", "suppress"])
async def test_history_writer_preserves_primary_through_session_cleanup(cancelled: "bool", cleanup: "str") -> "None":
    import asyncio
    from contextlib import asynccontextmanager
    from typing import Any
    from unittest.mock import AsyncMock, Mock

    from sqlspec.exceptions import NotNullViolationError

    from litestar_queues.backends.sqlspec.event_log import SQLSpecQueueEventLog
    from litestar_queues.events import EventHistoryConfig, QueueEvent

    primary = asyncio.CancelledError() if cancelled else NotNullViolationError("required column")
    driver = AsyncMock()
    driver.select.return_value = []
    driver.execute_many.side_effect = primary
    runtime_logger = Mock()
    runtime_logger.warning.side_effect = RuntimeError("diagnostic failure")

    @asynccontextmanager
    async def session() -> "Any":
        try:
            yield driver
        except (Exception, asyncio.CancelledError):  # noqa: BLE001 - exercise a context suppressing or replacing failures.
            if cleanup == "raise":
                message = "session exit failed"
                raise RuntimeError(message) from None

    log = SQLSpecQueueEventLog(
        session_factory=session,
        datetime_serializer=lambda value: value,
        config=EventHistoryConfig(strict=True),
        store=_store(),
        runtime_logger=runtime_logger,
    )
    record = log._record_from_event(QueueEvent(type="task.log", scope="task"))
    with pytest.raises(type(primary)) as error:
        await log._write_history_batch([record])
    assert error.value is primary
    driver.execute_many.assert_awaited_once()
    driver.rollback.assert_awaited_once()


def test_existing_record_lookup_is_parameterized_and_mapped() -> "None":
    store = _store()
    query = store.select_existing_event_ids(["event'one", "event-two"]).build()
    assert "event'one" not in query.sql
    assert " IN " in query.sql.upper()
    assert "detail" in query.sql

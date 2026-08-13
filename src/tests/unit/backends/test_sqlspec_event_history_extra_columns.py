"""Unit tests for adopter-declared extra columns on the SQLSpec event-history table."""

import pytest

pytest.importorskip("sqlspec")
pytest.importorskip("aiosqlite")

from typing import TYPE_CHECKING

from sqlspec.adapters.aiosqlite import AiosqliteConfig

from litestar_queues.backends.sqlspec import EventHistoryExtraColumn, SQLSpecBackendConfig
from litestar_queues.backends.sqlspec.event_log import create_event_log_store
from litestar_queues.backends.sqlspec.schema import EVENT_HISTORY_COLUMNS, validate_event_history_extra_columns
from litestar_queues.exceptions import QueueConfigurationError

if TYPE_CHECKING:
    from litestar_queues.events import QueueEventLog

_TENANT = EventHistoryExtraColumn(name="tenant_id", source="tenant_id", indexed=True)


def _store(*extra: "EventHistoryExtraColumn") -> "object":
    return create_event_log_store(
        AiosqliteConfig(connection_config={"database": ":memory:"}), queue_table_name="queue_task", extra_columns=extra
    )


def test_extra_column_declaration_validates() -> "None":
    columns = (EventHistoryExtraColumn(name="tenant_id", source="tenant_id", indexed=True),)

    assert validate_event_history_extra_columns(columns) == columns


def test_extra_column_source_defaults_are_explicit() -> "None":
    column = EventHistoryExtraColumn(name="project_id", source="project")

    assert column.indexed is False
    assert validate_event_history_extra_columns([column]) == (column,)


@pytest.mark.parametrize(
    "column",
    [
        pytest.param(EventHistoryExtraColumn(name="task_id", source="x"), id="reserved-name"),
        pytest.param(EventHistoryExtraColumn(name="drop table", source="x"), id="invalid-identifier"),
        pytest.param(EventHistoryExtraColumn(name="tenant_id", source=""), id="empty-source"),
        pytest.param(EventHistoryExtraColumn(name="", source="tenant_id"), id="empty-name"),
    ],
)
def test_extra_column_declaration_rejects(column: "EventHistoryExtraColumn") -> "None":
    with pytest.raises(QueueConfigurationError):
        validate_event_history_extra_columns((column,))


def test_extra_column_duplicate_names_rejected() -> "None":
    columns = (
        EventHistoryExtraColumn(name="tenant_id", source="tenant_id"),
        EventHistoryExtraColumn(name="tenant_id", source="account_id"),
    )

    with pytest.raises(QueueConfigurationError, match="tenant_id"):
        validate_event_history_extra_columns(columns)


def test_event_history_columns_are_shared_with_the_event_log_module() -> "None":
    from litestar_queues.backends.sqlspec.event_log import SQLSpecQueueEventLogStore

    assert "task_id" in EVENT_HISTORY_COLUMNS
    assert "detail" in EVENT_HISTORY_COLUMNS
    assert SQLSpecQueueEventLogStore is not None


def test_backend_config_validates_extra_columns() -> "None":
    config = SQLSpecBackendConfig(
        event_history_extra_columns=(EventHistoryExtraColumn(name="tenant_id", source="tenant_id", indexed=True),)
    )

    assert config.event_history_extra_columns == (
        EventHistoryExtraColumn(name="tenant_id", source="tenant_id", indexed=True),
    )

    with pytest.raises(QueueConfigurationError):
        SQLSpecBackendConfig(event_history_extra_columns=(EventHistoryExtraColumn(name="detail", source="x"),))


def test_store_without_extras_emits_unchanged_statements() -> "None":
    baseline = _store()

    statements = baseline.create_statements()  # type: ignore[attr-defined]
    template = baseline.insert_events_template()  # type: ignore[attr-defined]

    assert not any("tenant_id" in statement for statement in statements)
    assert "tenant_id" not in template
    assert template.count(":") == len(EVENT_HISTORY_COLUMNS)


def test_extra_columns_appear_in_ddl_and_insert_template() -> "None":
    store = _store(_TENANT)

    statements = store.create_statements()  # type: ignore[attr-defined]
    template = store.insert_events_template()  # type: ignore[attr-defined]

    assert any("tenant_id" in statement and statement.startswith("CREATE TABLE") for statement in statements)
    assert any("tenant_id" in statement and statement.startswith("CREATE INDEX") for statement in statements)
    assert ":tenant_id" in template
    assert any("tenant_id" in statement for statement in store.drop_statements())  # type: ignore[attr-defined]


def test_unindexed_extra_column_has_no_index_statement() -> "None":
    store = _store(EventHistoryExtraColumn(name="project_id", source="project_id"))

    statements = store.create_statements()  # type: ignore[attr-defined]

    assert any("project_id" in statement and statement.startswith("CREATE TABLE") for statement in statements)
    assert not any("project_id" in statement and statement.startswith("CREATE INDEX") for statement in statements)


def test_select_events_rejects_undeclared_extra_filter() -> "None":
    store = _store(_TENANT)

    with pytest.raises(ValueError, match="tenant_id"):
        store.select_events(extra={"unknown": "x"})  # type: ignore[attr-defined]


def test_select_events_accepts_declared_extra_filter() -> "None":
    store = _store(_TENANT)

    statement = store.select_events(extra={"tenant_id": "t-1"})  # type: ignore[attr-defined]

    assert "tenant_id" in statement.build(dialect="sqlite").sql


def test_sqlspec_event_log_still_satisfies_the_frozen_protocol() -> "None":
    """The extra filter is additive: the concrete store still matches ``QueueEventLog``."""
    from litestar_queues.backends.sqlspec.event_log import SQLSpecQueueEventLog

    def accepts_protocol(log: "QueueEventLog") -> "QueueEventLog":
        return log

    event_log = SQLSpecQueueEventLog.__new__(SQLSpecQueueEventLog)
    assert accepts_protocol(event_log) is event_log

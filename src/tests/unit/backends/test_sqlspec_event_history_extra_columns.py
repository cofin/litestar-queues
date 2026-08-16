"""Unit tests for adopter-declared extra columns on the SQLSpec event-history table."""

import pytest

pytest.importorskip("sqlspec")
pytest.importorskip("aiosqlite")

from typing import TYPE_CHECKING

from sqlspec.adapters.aiosqlite import AiosqliteConfig

from litestar_queues.backends.sqlspec import SQLSpecBackendConfig
from litestar_queues.backends.sqlspec.event_log import SQLSpecQueueEventLogStore, create_event_log_store
from litestar_queues.backends.sqlspec.schema import EVENT_HISTORY_COLUMNS
from litestar_queues.events import EventHistoryExtraColumn, QueueEventQuery, validate_event_history_extra_columns
from litestar_queues.exceptions import QueueConfigurationError

if TYPE_CHECKING:
    from litestar_queues.events import QueueEventLog

_TENANT = EventHistoryExtraColumn(name="tenant_id", source="tenant_id", indexed=True)


def _store(*extra: "EventHistoryExtraColumn") -> "SQLSpecQueueEventLogStore":
    return create_event_log_store(
        AiosqliteConfig(connection_config={"database": ":memory:"}), queue_table_name="queue_task", extra_columns=extra
    )


def _store_for_dialect(dialect: "str") -> "object":
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    config.statement_config.dialect = dialect
    return create_event_log_store(config, queue_table_name="queue_task")


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


@pytest.mark.parametrize("name", ["TASK_ID", "Detail", "Occurred_At"])
def test_extra_column_package_owned_names_are_case_insensitive(name: "str") -> "None":
    """Unquoted SQL identifiers fold case, so the collision check must too."""
    column = EventHistoryExtraColumn(name=name, source="x")

    with pytest.raises(QueueConfigurationError, match="package-owned"):
        validate_event_history_extra_columns((column,))


def test_extra_column_names_near_package_owned_names_are_allowed() -> "None":
    """Only exact package-owned names collide, not names that merely contain them."""
    columns = (
        EventHistoryExtraColumn(name="task_id_hash", source="a"),
        EventHistoryExtraColumn(name="detail_url", source="b"),
        EventHistoryExtraColumn(name="sub_queue", source="c"),
    )

    assert validate_event_history_extra_columns(columns) == columns


def test_extra_column_duplicate_names_are_case_insensitive() -> "None":
    """Two declarations differing only in case are the same physical column."""
    columns = (
        EventHistoryExtraColumn(name="tenant_id", source="tenant_id"),
        EventHistoryExtraColumn(name="TENANT_ID", source="account_id"),
    )

    with pytest.raises(QueueConfigurationError, match="Duplicate"):
        validate_event_history_extra_columns(columns)


@pytest.mark.parametrize("reserved", ["scope", "scope_key", "actor", "entity"])
def test_extra_column_reserved_dimension_names_rejected(reserved: "str") -> "None":
    """Names held for the built-in scoping dimensions cannot be claimed by adopters."""
    column = EventHistoryExtraColumn(name=reserved, source="x")

    with pytest.raises(QueueConfigurationError, match="reserved"):
        validate_event_history_extra_columns((column,))


@pytest.mark.parametrize("reserved", ["Scope", "SCOPE_KEY", "Actor", "ENTITY"])
def test_extra_column_reserved_names_are_case_insensitive(reserved: "str") -> "None":
    """Unquoted SQL identifiers are case-insensitive, so the reservation is too."""
    column = EventHistoryExtraColumn(name=reserved, source="x")

    with pytest.raises(QueueConfigurationError, match="reserved"):
        validate_event_history_extra_columns((column,))


def test_extra_column_names_near_reserved_words_are_allowed() -> "None":
    """Only the exact reserved names are held back."""
    columns = (
        EventHistoryExtraColumn(name="scope_id", source="scope_id"),
        EventHistoryExtraColumn(name="entity_id", source="entity_id"),
        EventHistoryExtraColumn(name="actor_name", source="actor_name"),
    )

    assert validate_event_history_extra_columns(columns) == columns


@pytest.mark.parametrize("name", ["actor_type", "actor_id", "ACTOR_ID", "Actor_Type"])
def test_actor_columns_are_package_owned(name: "str") -> "None":
    """The actor columns are real columns now, so they collide as package-owned names."""
    column = EventHistoryExtraColumn(name=name, source="x")

    with pytest.raises(QueueConfigurationError, match="package-owned"):
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

    statements = baseline.create_statements()
    template = baseline.insert_events_template()

    assert not any("tenant_id" in statement for statement in statements)
    assert "tenant_id" not in template
    assert template.count(":") == len(EVENT_HISTORY_COLUMNS)


def test_mysql_event_history_uses_inline_indexes_and_backtick_quoting() -> "None":
    statements = _store_for_dialect("mysql").create_statements()  # type: ignore[attr-defined]

    assert len(statements) == 1
    assert statements[0].startswith("CREATE TABLE IF NOT EXISTS `queue_task_event_history`")
    assert "INDEX `ix_queue_task_event_history_task_id`" in statements[0]
    assert "CREATE INDEX IF NOT EXISTS" not in statements[0]


def test_mysql_event_history_bounds_generated_index_names() -> "None":
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    config.statement_config.dialect = "mysql"
    store = create_event_log_store(config, queue_table_name="queue_task_mysql_aiomysql_637b5b4678")

    assert len(store._index_name("occurred_at")) <= 63
    assert store._quoted_index_name("occurred_at") in store.create_statements()[0]


def test_mssql_event_history_bypasses_sqlglot_for_native_datetime_type() -> "None":
    statements = _store_for_dialect("tsql").create_statements()  # type: ignore[attr-defined]

    assert statements[0].lstrip().startswith("IF OBJECT_ID")
    assert "DATETIME2(6)" in statements[0]
    assert all("CREATE INDEX IF NOT EXISTS" not in statement for statement in statements[1:])


def test_oracle_event_history_prefixes_reserved_level_column() -> "None":
    statements = _store_for_dialect("oracle").create_statements()  # type: ignore[attr-defined]

    store = _store_for_dialect("oracle")
    assert "EVENT_LEVEL VARCHAR(255)" in statements[0]
    assert "EVENT_LEVEL" in store.insert_events_template()  # type: ignore[attr-defined]
    selected = store.select_events().build(dialect="oracle").sql  # type: ignore[attr-defined]
    assert "event_level" in selected
    assert "event_level AS level" not in selected
    assert "(TASK_ID, SEQUENCE, OCCURRED_AT)" in statements[1]


def test_spanner_event_history_uses_backtick_quoted_identifiers() -> "None":
    statements = _store_for_dialect("spanner").create_statements()  # type: ignore[attr-defined]

    assert statements[0].startswith("CREATE TABLE IF NOT EXISTS `queue_task_event_history`")


def test_extra_columns_appear_in_ddl_and_insert_template() -> "None":
    store = _store(_TENANT)

    statements = store.create_statements()
    template = store.insert_events_template()

    assert any("tenant_id" in statement and statement.startswith("CREATE TABLE") for statement in statements)
    assert any("tenant_id" in statement and statement.startswith("CREATE INDEX") for statement in statements)
    assert ":tenant_id" in template
    assert any("tenant_id" in statement for statement in store.drop_statements())


def test_unindexed_extra_column_has_no_index_statement() -> "None":
    store = _store(EventHistoryExtraColumn(name="project_id", source="project_id"))

    statements = store.create_statements()

    assert any("project_id" in statement and statement.startswith("CREATE TABLE") for statement in statements)
    assert not any("project_id" in statement and statement.startswith("CREATE INDEX") for statement in statements)


def test_actor_columns_are_created_and_indexed() -> "None":
    store = _store()

    statements = store.create_statements()
    template = store.insert_events_template()

    create_table = next(statement for statement in statements if statement.startswith("CREATE TABLE"))
    assert "actor_type" in create_table
    assert "actor_id" in create_table
    assert any(
        statement.startswith("CREATE INDEX") and "actor_id" in statement and "occurred_at" in statement
        for statement in statements
    )
    assert ":actor_type" in template
    assert ":actor_id" in template
    assert any("actor_id" in statement for statement in store.drop_statements())


def test_actor_name_is_not_persisted() -> "None":
    """Display names go stale against the event they are stamped on, so only type and id persist."""
    store = _store()

    statements = store.create_statements()

    assert "actor_name" not in next(statement for statement in statements if statement.startswith("CREATE TABLE"))


def test_select_events_filters_on_actor() -> "None":
    store = _store()

    rendered = (
        store
        .select_events(QueueEventQuery(), extra={"actor_id": "u-1", "actor_type": "user"})
        .build(dialect="sqlite")
        .sql
    )

    assert "actor_id" in rendered
    assert "actor_type" in rendered


def test_select_events_rejects_undeclared_extra_filter() -> "None":
    store = _store(_TENANT)

    with pytest.raises(QueueConfigurationError):
        store.select_events(QueueEventQuery(), extra={"unknown": "x"})


def test_select_events_accepts_declared_extra_filter() -> "None":
    store = _store(_TENANT)

    statement = store.select_events(QueueEventQuery(), extra={"tenant_id": "t-1"})

    assert "tenant_id" in statement.build(dialect="sqlite").sql


def test_sqlspec_event_log_still_satisfies_the_frozen_protocol() -> "None":
    """The extra filter is additive: the concrete store still matches ``QueueEventLog``."""
    from litestar_queues.backends.sqlspec.event_log import SQLSpecQueueEventLog

    def accepts_protocol(log: "QueueEventLog") -> "QueueEventLog":
        return log

    event_log = SQLSpecQueueEventLog.__new__(SQLSpecQueueEventLog)
    assert accepts_protocol(event_log) is event_log

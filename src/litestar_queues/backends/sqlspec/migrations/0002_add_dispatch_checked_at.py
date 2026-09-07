"""Add durable dispatch-repair scan progress.

Spanner statements require its native administrative DDL API; SQLSpec's generic
migration executor sends statements through DML. Generating this migration never
executes its DDL, including when a migration loader probes the downgrade.
"""

from inspect import isawaitable

# Migration and store form one schema implementation boundary.
# ruff: noqa: SLF001
from typing import TYPE_CHECKING, Any, cast

from sqlspec.exceptions import SQLSpecError
from sqlspec.utils.text import split_qualified_identifier

from litestar_queues.backends.sqlspec.stores.factory import create_queue_store

if TYPE_CHECKING:
    from sqlspec.migrations.context import MigrationContext

    from litestar_queues.backends.sqlspec.stores.base import SQLSpecQueueStore

__all__ = ("down", "up")


async def _inspect(context: "MigrationContext | None") -> "tuple[SQLSpecQueueStore, set[str], set[str]]":
    if context is None or context.config is None:
        msg = "Dispatch repair migration requires a SQLSpec adapter configuration."
        raise SQLSpecError(msg)
    config = cast("Any", context.config)
    store = create_queue_store(config, manage_schema=bool(getattr(config, "manage_schema", True)))
    if not store._manage_schema:
        return store, set(), set()
    driver = context.driver
    if driver is None:
        msg = "Dispatch repair migration requires an active migration driver for schema inspection."
        raise SQLSpecError(msg)
    parts = split_qualified_identifier(store.dispatch_repair_table_name)
    table = parts[-1]
    schema = ".".join(parts[:-1]) or None
    dictionary = driver.data_dictionary
    columns_result = dictionary.get_columns(driver, table=table, schema=schema)
    columns = await columns_result if isawaitable(columns_result) else columns_result
    indexes_result: Any
    if store.data_dictionary_dialect == "spanner":
        # SQLSpec 0.62.1 get_indexes requests an unsupported is_primary_key
        # catalog column. The migration requires only the supported name field.
        indexes_result = driver.select(
            "SELECT INDEX_NAME AS index_name FROM INFORMATION_SCHEMA.INDEXES "
            "WHERE TABLE_NAME = @table_name AND TABLE_SCHEMA = @schema_name",
            table_name=table,
            schema_name=schema or "",
        )
    else:
        indexes_result = dictionary.get_indexes(driver, table=table, schema=schema)
    indexes = await indexes_result if isawaitable(indexes_result) else indexes_result
    normalize = str.casefold if store._data_dictionary_dialect_name() in {"oracle", "sqlite"} else str
    return (
        store,
        {normalize(str(row.get("column_name", ""))) for row in columns},
        {normalize(str(row["index_name"])) for row in indexes},
    )


async def up(context: "MigrationContext | None" = None) -> "list[str]":
    """Return only the missing additive column and index statements."""
    store, columns, indexes = await _inspect(context)
    if not store._manage_schema:
        return []
    normalize = str.casefold if store._data_dictionary_dialect_name() in {"oracle", "sqlite"} else str
    statements = []
    if normalize(store.dispatch_checked_column_name) not in columns:
        statements.append(store.dispatch_checked_column_sql())
    if normalize(store.dispatch_repair_index_name) not in indexes:
        statements.append(store.dispatch_repair_index_sql())
    return statements


async def down(context: "MigrationContext | None" = None) -> "list[str]":
    """Remove only the repair artifacts present in the inspected schema."""
    store, columns, indexes = await _inspect(context)
    if not store._manage_schema:
        return []
    normalize = str.casefold if store._data_dictionary_dialect_name() in {"oracle", "sqlite"} else str
    statements = []
    if normalize(store.dispatch_repair_index_name) in indexes:
        statements.append(store.drop_dispatch_repair_index_sql())
    if normalize(store.dispatch_checked_column_name) in columns:
        statements.append(store.dispatch_checked_column_sql(drop=True))
    return statements

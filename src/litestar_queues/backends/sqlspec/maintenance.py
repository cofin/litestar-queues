"""SQLSpec-backed distributed maintenance coordination store."""

from typing import TYPE_CHECKING, Any

from sqlspec import sql

from litestar_queues.backends.sqlspec.schema import maintenance_table_name_for, validate_table_name
from litestar_queues.backends.sqlspec.stores.base import SQLSpecQueueStore, _adapter_name
from litestar_queues.backends.sqlspec.stores.factory import _adapter_store_type

if TYPE_CHECKING:
    from sqlspec.builder import Delete, Insert, Select, Update

    from litestar_queues.backends.sqlspec._typing import DatetimeParam, SQLSpecStoreConfig

__all__ = (
    "SQLSpecMaintenanceStore",
    "SpannerMaintenanceStore",
    "create_maintenance_store",
    "resolve_maintenance_table_name",
)


class SQLSpecMaintenanceStore(SQLSpecQueueStore):
    """SQLSpec statement store for distributed maintenance coordination.

    The table has one row per operation name carrying the current holder's
    token and an expiry timestamp. Acquire is a compare-and-set; release deletes
    only the row that still matches name and token, so a stale holder can never
    remove a successor's ownership record.
    """

    __slots__ = ()

    def __init__(self, *args: "Any", **kwargs: "Any") -> "None":
        super().__init__(*args, **kwargs)
        self._column_map = {}

    def create_table_statement(self) -> "str":
        """Return the maintenance coordination ``CREATE TABLE`` statement."""
        if not self._manage_schema:
            return ""
        return self._create_table_sql()

    def companion_indexes(self) -> "tuple[tuple[str, str], ...]":
        """Return empty companion index tuple (maintenance uses primary key only)."""
        return ()

    def create_statements(self) -> "list[str]":
        """Return statements that create the maintenance coordination table."""
        statement = self.create_table_statement()
        return [statement] if statement else []

    def prepare_schema_sync(self, driver: "Any") -> "None":
        """Prevent inherited adapter queue stores from running queue schema prep."""

    async def prepare_schema_async(self, driver: "Any") -> "None":
        """Prevent inherited adapter queue stores from running queue schema prep."""

    def reconcile_storage_options_sync(self, driver: "Any") -> "None":
        """Prevent inherited Postgres or CockroachDB queue options from running on maintenance."""

    async def reconcile_storage_options_async(self, driver: "Any") -> "None":
        """Prevent inherited Postgres or CockroachDB queue options from running on maintenance."""

    def drop_statements(self) -> "list[str]":
        """Return statements that drop the maintenance coordination table."""
        if not self._manage_schema:
            return []
        return [self._drop_table_sql()]

    def acquire_update(
        self, *, name: "str", token: "str", expires_at: "DatetimeParam", now: "DatetimeParam"
    ) -> "Update":
        """Return an UPDATE that claims the named operation when ownership expired."""
        return (
            sql
            .update(self.table_name)
            .set(token=token, expires_at=expires_at)
            .where_eq("name", name)
            .where("expires_at <= :coordination_now", coordination_now=now)
        )

    def insert_coordination(self, *, name: "str", token: "str", expires_at: "DatetimeParam") -> "Insert":
        """Return an INSERT for a new maintenance ownership row."""
        return (
            sql
            .insert(self.table_name)
            .columns("name", "token", "expires_at")
            .values(name=name, token=token, expires_at=expires_at)
        )

    def select_coordination_token(self, *, name: "str") -> "Select":
        """Return a SELECT of the current ownership token for an operation."""
        return sql.select("token").from_(self.table_name).where_eq("name", name)

    def count_coordination(self, *, name: "str", token: "str") -> "Select":
        """Return a COUNT of ownership rows matching an operation and token."""
        return (
            sql
            .select(sql.raw("COUNT(*) AS coordination_count"))
            .from_(self.table_name)
            .where_eq("name", name)
            .where_eq("token", token)
        )

    def delete_coordination(self, *, name: "str", token: "str") -> "Delete":
        """Return a DELETE that releases ownership held under ``token``."""
        return sql.delete(self.table_name).where_eq("name", name).where_eq("token", token)

    def _is_oracle(self) -> "bool":
        return "oracle" in (self._data_dictionary_dialect_name() or "").lower()

    def _rendered_create(self, *, if_not_exists: "bool") -> "str":
        statement = sql.create_table(self.table_name)
        if if_not_exists:
            statement = statement.if_not_exists()
        statement = (
            statement
            .column("name", self._indexed_text_type(), primary_key=True)
            .column("token", self._indexed_text_type(), not_null=True)
            .column("expires_at", self._timestamp_type(), not_null=True)
        )
        rendered = self._to_sql(statement)
        unsplit_target = self._quote_unsplit_identifier(self.table_name)
        split_target = self._quoted_table_name()
        if unsplit_target != split_target:
            rendered = rendered.replace(unsplit_target, split_target, 1)
        return rendered

    def _create_table_sql(self) -> "str":
        if self._is_oracle():
            return _oracle_ddl_block(self._rendered_create(if_not_exists=False), ignored_code=-955)
        if type(self).identifier_quote_style == "none":
            columns = (
                f"{self._quoted_col('name')} {self._indexed_text_type()} PRIMARY KEY, "
                f"{self._quoted_col('token')} {self._indexed_text_type()} NOT NULL, "
                f"{self._quoted_col('expires_at')} {self._timestamp_type()} NOT NULL"
            )
            return (
                f"IF OBJECT_ID(N'{self.table_name}', N'U') IS NULL "
                f"BEGIN CREATE TABLE {self._quoted_table_name()} ({columns}) END"
            )
        return self._rendered_create(if_not_exists=True)

    def _drop_table_sql(self) -> "str":
        if self._is_oracle():
            return _oracle_ddl_block(f"DROP TABLE {self._quoted_table_name()}", ignored_code=-942)
        if type(self).identifier_quote_style == "none":
            return f"IF OBJECT_ID(N'{self.table_name}', N'U') IS NOT NULL DROP TABLE {self._quoted_table_name()};"
        return self._to_sql(sql.drop_table(self.table_name).if_exists())


class SpannerMaintenanceStore(SQLSpecMaintenanceStore):
    """Spanner maintenance coordination store with native-compatible DDL."""

    __slots__ = ()

    data_dictionary_dialect = "spanner"
    identifier_quote_style = "backtick"
    skip_cleanup_rollback = True

    def create_table_statement(self) -> "str":
        """Return the Spanner maintenance coordination ``CREATE TABLE`` statement."""
        if not self._manage_schema:
            return ""
        columns = (
            f"{self._quote_identifier('name')} {self._indexed_text_type()} NOT NULL",
            f"{self._quote_identifier('token')} {self._indexed_text_type()} NOT NULL",
            f"{self._quote_identifier('expires_at')} {self._timestamp_type()} NOT NULL",
        )
        column_sql = ",\n  ".join(columns)
        return (
            f"CREATE TABLE {self._quoted_table_name()} (\n  {column_sql}\n) "
            f"PRIMARY KEY ({self._quote_identifier('name')})"
        )

    def drop_statements(self) -> "list[str]":
        """Return the Spanner maintenance coordination DROP TABLE statement."""
        if not self._manage_schema:
            return []
        return [f"DROP TABLE {self._quoted_table_name()}"]

    def create_schema_for_config(self, config: "Any") -> "None":
        """Create the coordination table through Spanner's native DDL operation API."""
        from litestar_queues.backends.sqlspec.stores.spanner import _execute_spanner_ddl

        if not self._manage_schema:
            return

        get_database = getattr(config, "get_database", None)
        if not callable(get_database):
            msg = "Spanner maintenance schema creation requires a SQLSpec SpannerSyncConfig."
            raise TypeError(msg)
        database = get_database()
        for statement in self.create_statements():
            _execute_spanner_ddl(database, statement)

    def _string_type(self, length: "int | None" = None) -> "str":
        return "STRING(MAX)" if length is None else f"STRING({length})"

    def _timestamp_type(self) -> "str":
        return "TIMESTAMP"


def _maintenance_store_type_for(adapter_store_type: "type[SQLSpecQueueStore]") -> "type[SQLSpecMaintenanceStore]":
    """Return a maintenance store class that inherits the adapter's behavior.

    Mixing the coordination DDL and queries with the per-adapter queue store
    gives the table the adapter's timestamp type, identifier quoting, and datetime
    binding (e.g. MySQL ``DATETIME(6)`` and backtick quoting) instead of the
    generic base defaults.

    Returns:
        A maintenance store subclass for ``adapter_store_type``.
    """
    if adapter_store_type is SQLSpecQueueStore:
        return SQLSpecMaintenanceStore
    return type(
        f"{adapter_store_type.__name__}Maintenance", (SQLSpecMaintenanceStore, adapter_store_type), {"__slots__": ()}
    )


def create_maintenance_store(
    config: "SQLSpecStoreConfig",
    *,
    queue_table_name: "str",
    maintenance_table_name: "str | None" = None,
    manage_schema: "bool" = True,
) -> "SQLSpecMaintenanceStore":
    """Create a maintenance coordination store for a SQLSpec adapter configuration.

    Returns:
        A store configured for the resolved maintenance table.
    """
    maintenance_store_type = (
        SpannerMaintenanceStore
        if _adapter_name(config) == "spanner"
        else _maintenance_store_type_for(_adapter_store_type(config))
    )
    return maintenance_store_type(
        config,
        table_name=resolve_maintenance_table_name(queue_table_name, maintenance_table_name=maintenance_table_name),
        manage_schema=manage_schema,
    )


def resolve_maintenance_table_name(queue_table_name: "str", *, maintenance_table_name: "str | None" = None) -> "str":
    """Resolve the SQLSpec maintenance table name for a queue table.

    Returns:
        The explicit maintenance table name, or the name derived from the queue table.
    """
    if maintenance_table_name is not None:
        return validate_table_name(maintenance_table_name)
    return maintenance_table_name_for(queue_table_name)


def _oracle_ddl_block(statement: "str", *, ignored_code: "int") -> "str":
    escaped = statement.replace("'", "''")
    return f"""
    BEGIN
        EXECUTE IMMEDIATE '{escaped}';
    EXCEPTION
        WHEN OTHERS THEN
            IF SQLCODE != {ignored_code} THEN
                RAISE;
            END IF;
    END;
    """

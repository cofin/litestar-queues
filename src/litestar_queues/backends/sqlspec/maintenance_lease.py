"""SQLSpec-backed distributed maintenance lease store."""

from typing import TYPE_CHECKING, Any

from sqlspec import sql

from litestar_queues.backends.sqlspec.schema import maintenance_lease_table_name_for, validate_table_name
from litestar_queues.backends.sqlspec.stores.base import SQLSpecQueueStore, _adapter_name
from litestar_queues.backends.sqlspec.stores.factory import _adapter_store_type

if TYPE_CHECKING:
    from sqlspec.builder import Delete, Insert, Select, Update

    from litestar_queues.backends.sqlspec._typing import DatetimeParam, SQLSpecStoreConfig

__all__ = (
    "SQLSpecMaintenanceLeaseStore",
    "SpannerMaintenanceLeaseStore",
    "create_maintenance_lease_store",
    "resolve_maintenance_lease_table_name",
)


class SQLSpecMaintenanceLeaseStore(SQLSpecQueueStore):
    """SQLSpec statement store for the distributed maintenance lease table.

    The lease table has one row per lease name carrying the current holder's
    token and an expiry timestamp. Acquire is a compare-and-set; release deletes
    only the row that still matches name and token, so a stale holder can never
    release a successor's lease.
    """

    __slots__ = ()

    def __init__(self, *args: "Any", **kwargs: "Any") -> "None":
        super().__init__(*args, **kwargs)
        self._column_map = {}

    def create_statements(self) -> "list[str]":
        """Return statements that create the maintenance-lease table."""
        if not self._manage_schema:
            return []
        return [self._create_lease_table_sql()]

    def drop_statements(self) -> "list[str]":
        """Return statements that drop the maintenance-lease table."""
        if not self._manage_schema:
            return []
        return [self._drop_lease_table_sql()]

    def acquire_update(
        self, *, name: "str", token: "str", expires_at: "DatetimeParam", now: "DatetimeParam"
    ) -> "Update":
        """Return an UPDATE that claims the named lease when the stored row is expired."""
        return (
            sql
            .update(self.table_name)
            .set(token=token, expires_at=expires_at)
            .where_eq("name", name)
            .where("expires_at <= :lease_now", lease_now=now)
        )

    def insert_lease(self, *, name: "str", token: "str", expires_at: "DatetimeParam") -> "Insert":
        """Return an INSERT for a new lease row."""
        return (
            sql
            .insert(self.table_name)
            .columns("name", "token", "expires_at")
            .values(name=name, token=token, expires_at=expires_at)
        )

    def select_lease_token(self, *, name: "str") -> "Select":
        """Return a SELECT of the current token for a lease name."""
        return sql.select("token").from_(self.table_name).where_eq("name", name)

    def count_lease(self, *, name: "str", token: "str") -> "Select":
        """Return a COUNT of rows matching a lease name and token."""
        return (
            sql
            .select(sql.raw("COUNT(*) AS lease_count"))
            .from_(self.table_name)
            .where_eq("name", name)
            .where_eq("token", token)
        )

    def release_delete(self, *, name: "str", token: "str") -> "Delete":
        """Return a DELETE that releases a lease held under ``token``."""
        return sql.delete(self.table_name).where_eq("name", name).where_eq("token", token)

    def _is_oracle(self) -> "bool":
        return "oracle" in (self._data_dictionary_dialect_name() or "").lower()

    def _rendered_create(self, *, if_not_exists: "bool") -> "str":
        # Render the CREATE TABLE with the same sqlglot builder path the queue
        # store uses, so column types and identifier quoting are dialect-correct.
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

    def _create_lease_table_sql(self) -> "str":
        # Oracle and SQL Server both use identifier_quote_style="none", so Oracle
        # must be checked first. The sqlglot builder renders valid DDL for every
        # dialect EXCEPT SQL Server, whose DATETIME2 column type it cannot parse,
        # so SQL Server is the only hand-rolled path (mirroring the queue store).
        if self._is_oracle():
            return _oracle_ddl_block(self._rendered_create(if_not_exists=False), ignored_code=-955)
        if type(self).identifier_quote_style == "none":  # SQL Server
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

    def _drop_lease_table_sql(self) -> "str":
        if self._is_oracle():
            return _oracle_ddl_block(f"DROP TABLE {self._quoted_table_name()}", ignored_code=-942)
        if type(self).identifier_quote_style == "none":  # SQL Server
            return f"IF OBJECT_ID(N'{self.table_name}', N'U') IS NOT NULL DROP TABLE {self._quoted_table_name()};"
        return self._to_sql(sql.drop_table(self.table_name).if_exists())


class SpannerMaintenanceLeaseStore(SQLSpecMaintenanceLeaseStore):
    """Spanner maintenance-lease store with native-compatible DDL."""

    __slots__ = ()

    data_dictionary_dialect = "spanner"
    identifier_quote_style = "backtick"
    skip_cleanup_rollback = True

    def create_statements(self) -> "list[str]":
        """Return the Spanner maintenance-lease CREATE TABLE statement."""
        if not self._manage_schema:
            return []
        columns = (
            f"{self._quote_identifier('name')} {self._indexed_text_type()} NOT NULL",
            f"{self._quote_identifier('token')} {self._indexed_text_type()} NOT NULL",
            f"{self._quote_identifier('expires_at')} {self._timestamp_type()} NOT NULL",
        )
        column_sql = ",\n  ".join(columns)
        return [
            f"CREATE TABLE {self._quoted_table_name()} (\n  {column_sql}\n) "
            f"PRIMARY KEY ({self._quote_identifier('name')})"
        ]

    def drop_statements(self) -> "list[str]":
        """Return the Spanner maintenance-lease DROP TABLE statement."""
        if not self._manage_schema:
            return []
        return [f"DROP TABLE {self._quoted_table_name()}"]

    def create_schema_for_config(self, config: "Any") -> "None":
        """Create the lease table through Spanner's native DDL operation API.

        Returns:
            None.
        """
        if not self._manage_schema:
            return
        from litestar_queues.backends.sqlspec.stores.spanner.store import _execute_spanner_ddl

        get_database = getattr(config, "get_database", None)
        if not callable(get_database):
            msg = "Spanner maintenance-lease schema creation requires a SQLSpec SpannerSyncConfig."
            raise TypeError(msg)
        database = get_database()
        for statement in self.create_statements():
            _execute_spanner_ddl(database, statement)

    def _string_type(self, length: "int | None" = None) -> "str":
        return "STRING(MAX)" if length is None else f"STRING({length})"

    def _timestamp_type(self) -> "str":
        return "TIMESTAMP"


def _lease_store_type_for(adapter_store_type: "type[SQLSpecQueueStore]") -> "type[SQLSpecMaintenanceLeaseStore]":
    """Return a lease store class that inherits the adapter's store behavior.

    Mixing the lease DDL/queries with the per-adapter queue store class gives the
    lease table the adapter's timestamp type, identifier quoting, and datetime
    binding (e.g. MySQL ``DATETIME(6)`` and backtick quoting) instead of the
    generic base defaults.

    Returns:
        A lease store subclass for ``adapter_store_type``.
    """
    if adapter_store_type is SQLSpecQueueStore:
        return SQLSpecMaintenanceLeaseStore
    return type(
        f"{adapter_store_type.__name__}MaintenanceLease",
        (SQLSpecMaintenanceLeaseStore, adapter_store_type),
        {"__slots__": ()},
    )


def create_maintenance_lease_store(
    config: "SQLSpecStoreConfig",
    *,
    queue_table_name: "str",
    maintenance_lease_table_name: "str | None" = None,
    manage_schema: "bool" = True,
) -> "SQLSpecMaintenanceLeaseStore":
    """Create a maintenance-lease store for a SQLSpec adapter configuration.

    Returns:
        A lease store configured for the resolved lease table.
    """
    lease_store_type = (
        SpannerMaintenanceLeaseStore
        if _adapter_name(config) == "spanner"
        else _lease_store_type_for(_adapter_store_type(config))
    )
    return lease_store_type(
        config,
        table_name=resolve_maintenance_lease_table_name(
            queue_table_name, maintenance_lease_table_name=maintenance_lease_table_name
        ),
        manage_schema=manage_schema,
    )


def resolve_maintenance_lease_table_name(
    queue_table_name: "str", *, maintenance_lease_table_name: "str | None" = None
) -> "str":
    """Resolve the SQLSpec maintenance-lease table name for a queue table.

    Returns:
        The explicit lease table name, or the derived queue-table lease name.
    """
    if maintenance_lease_table_name is not None:
        return validate_table_name(maintenance_lease_table_name)
    return maintenance_lease_table_name_for(queue_table_name)


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

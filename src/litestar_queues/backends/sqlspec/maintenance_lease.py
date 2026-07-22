"""SQLSpec-backed distributed maintenance lease store."""

from typing import TYPE_CHECKING, Any

from sqlspec import sql

from litestar_queues.backends.sqlspec.schema import maintenance_lease_table_name_for, validate_table_name
from litestar_queues.backends.sqlspec.stores.base import SQLSpecQueueStore
from litestar_queues.backends.sqlspec.stores.factory import _adapter_store_type

if TYPE_CHECKING:
    from sqlspec.builder import Delete, Insert, Select, Update

    from litestar_queues.backends.sqlspec._typing import DatetimeParam, SQLSpecStoreConfig

__all__ = ("SQLSpecMaintenanceLeaseStore", "create_maintenance_lease_store", "resolve_maintenance_lease_table_name")


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

    def _lease_columns_sql(self, *, quoted: "bool") -> "str":
        column = self._quoted_col if quoted else self._col
        return (
            f"{column('name')} {self._indexed_text_type()} PRIMARY KEY, "
            f"{column('token')} {self._indexed_text_type()} NOT NULL, "
            f"{column('expires_at')} {self._timestamp_type()} NOT NULL"
        )

    def _is_oracle(self) -> "bool":
        return "oracle" in (self._data_dictionary_dialect_name() or "").lower()

    def _create_lease_table_sql(self) -> "str":
        # Build raw DDL rather than the SQL builder so adapter-specific column
        # types (e.g. SQL Server ``DATETIME2(6)``) that sqlglot cannot render in
        # a CREATE TABLE are emitted verbatim, mirroring the queue store DDL.
        table = self._quoted_table_name()
        if type(self).identifier_quote_style == "none":  # SQL Server
            columns = self._lease_columns_sql(quoted=True)
            return f"IF OBJECT_ID(N'{self.table_name}', N'U') IS NULL BEGIN CREATE TABLE {table} ({columns}) END"
        if self._is_oracle():
            columns = self._lease_columns_sql(quoted=False)
            return (
                f"BEGIN EXECUTE IMMEDIATE 'CREATE TABLE {self.table_name} ({columns})'; "
                "EXCEPTION WHEN OTHERS THEN IF SQLCODE != -955 THEN RAISE; END IF; END;"
            )
        columns = self._lease_columns_sql(quoted=True)
        return f"CREATE TABLE IF NOT EXISTS {table} ({columns})"

    def _drop_lease_table_sql(self) -> "str":
        table = self._quoted_table_name()
        if type(self).identifier_quote_style == "none":  # SQL Server
            return f"IF OBJECT_ID(N'{self.table_name}', N'U') IS NOT NULL DROP TABLE {table};"
        if self._is_oracle():
            return (
                f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {self.table_name}'; "
                "EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
            )
        return f"DROP TABLE IF EXISTS {table}"


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
    lease_store_type = _lease_store_type_for(_adapter_store_type(config))
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

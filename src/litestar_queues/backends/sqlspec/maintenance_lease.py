"""SQLSpec-backed distributed maintenance lease store."""

from typing import TYPE_CHECKING, Any

from sqlspec import sql

from litestar_queues.backends.sqlspec.schema import maintenance_lease_table_name_for, validate_table_name
from litestar_queues.backends.sqlspec.stores.base import SQLSpecQueueStore

if TYPE_CHECKING:
    from sqlspec.builder import CreateTable, Delete, DropTable, Insert, Select, Update

    from litestar_queues.backends.sqlspec._typing import DatetimeParam, SQLSpecStoreConfig

__all__ = (
    "SQLSpecMaintenanceLeaseStore",
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
        return [self._to_sql(sql.drop_table(self.table_name).if_exists())]

    def acquire_update(self, *, name: "str", token: "str", expires_at: "DatetimeParam", now: "DatetimeParam") -> "Update":
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
        return sql.insert(self.table_name).columns("name", "token", "expires_at").values(
            name=name, token=token, expires_at=expires_at
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

    def _create_lease_table_statement(self) -> "CreateTable":
        return (
            sql
            .create_table(self.table_name)
            .if_not_exists()
            .column("name", self._indexed_text_type(), primary_key=True)
            .column("token", self._indexed_text_type(), not_null=True)
            .column("expires_at", self._timestamp_type(), not_null=True)
        )

    def _create_lease_table_sql(self) -> "str":
        rendered = self._to_sql(self._create_lease_table_statement())
        unsplit_target = self._quote_unsplit_identifier(self.table_name)
        split_target = self._quoted_table_name()
        if unsplit_target != split_target:
            rendered = rendered.replace(unsplit_target, split_target, 1)
        return rendered

    def _to_sql(self, statement: "CreateTable | DropTable | Any") -> "str":
        built = statement.build(dialect=self.dialect_name)
        return built.sql


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
    return SQLSpecMaintenanceLeaseStore(
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

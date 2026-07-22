"""SQLSpec-backed forever-uniqueness tombstone store.

The tombstone table is separate from the queue table so routine terminal
cleanup never removes a reservation. Reservation atomicity is provided by the
identity-key PRIMARY KEY plus an optimistic insert with a unique-violation
fallback (the same primitive the queue's ``task_key`` uses), so it is portable
across every SQLSpec adapter, including single-writer sync drivers.
"""

from typing import TYPE_CHECKING, Any

from sqlspec import sql
from sqlspec.utils.text import split_qualified_identifier

from litestar_queues.backends.sqlspec.schema import uniqueness_table_name_for, validate_table_name
from litestar_queues.backends.sqlspec.stores._families import _NVARCHAR_MAX_THRESHOLD, _quote_tsql_identifier
from litestar_queues.backends.sqlspec.stores.base import SQLSpecQueueStore, _adapter_name

if TYPE_CHECKING:
    from sqlspec.builder import CreateTable, Delete, Insert, Select

    from litestar_queues.backends.sqlspec._typing import SQLSpecStoreConfig

__all__ = (
    "MssqlQueueTombstoneStore",
    "OracleQueueTombstoneStore",
    "SQLSpecQueueTombstoneStore",
    "SpannerQueueTombstoneStore",
    "create_tombstone_store",
    "resolve_uniqueness_table_name",
)

_TOMBSTONE_COLUMNS = ("identity_key", "task_id", "task_name", "created_at")
_MSSQL_ADAPTERS = frozenset({"pymssql", "mssql_python", "arrow_odbc"})


class SQLSpecQueueTombstoneStore(SQLSpecQueueStore):
    """SQLSpec statement store for forever-uniqueness tombstones."""

    __slots__ = ()

    def __init__(self, *args: "Any", **kwargs: "Any") -> "None":
        super().__init__(*args, **kwargs)
        self._column_map = {}

    def create_statements(self) -> "list[str]":
        """Return statements that create the tombstone table."""
        if not self._manage_schema:
            return []
        return [self._create_tombstone_table_sql()]

    def drop_statements(self) -> "list[str]":
        """Return statements that drop the tombstone table."""
        if not self._manage_schema:
            return []
        return [self._to_sql(sql.drop_table(self.table_name).if_exists())]

    def select_owner(self, key: "str") -> "Select":
        """Return a SELECT for the tombstone owning ``key``."""
        return sql.select(*_TOMBSTONE_COLUMNS).from_(self.table_name).where_eq("identity_key", key)

    def insert_reservation(self, values: "dict[str, Any]") -> "Insert":
        """Return an INSERT that reserves an identity key."""
        return sql.insert(self.table_name).columns(*values.keys()).values(**values)

    def count_by_key(self, key: "str", *, expected_task_id: "str | None" = None) -> "Select":
        """Return a COUNT for a tombstone key (reset uses count-then-delete)."""
        statement = (
            sql.select(sql.raw("COUNT(*) AS tombstone_count")).from_(self.table_name).where_eq("identity_key", key)
        )
        if expected_task_id is not None:
            statement = statement.where_eq("task_id", expected_task_id)
        return statement

    def delete_by_key(self, key: "str", *, expected_task_id: "str | None" = None) -> "Delete":
        """Return a DELETE removing the tombstone for ``key``."""
        statement = sql.delete(self.table_name).where_eq("identity_key", key)
        if expected_task_id is not None:
            statement = statement.where_eq("task_id", expected_task_id)
        return statement

    def _create_tombstone_table_statement(self, *, if_not_exists: "bool" = True) -> "CreateTable":
        statement = sql.create_table(self.table_name)
        if if_not_exists:
            statement = statement.if_not_exists()
        return (
            statement
            .column("identity_key", self._indexed_text_type(), primary_key=True)
            .column("task_id", self._id_type(), not_null=True)
            .column("task_name", self._indexed_text_type(), not_null=True)
            .column("created_at", self._timestamp_type(), not_null=True)
        )

    def _create_tombstone_table_sql(self) -> "str":
        rendered = self._to_sql(self._create_tombstone_table_statement())
        unsplit_target = self._quote_unsplit_identifier(self.table_name)
        split_target = self._quoted_table_name()
        if unsplit_target != split_target:
            rendered = rendered.replace(unsplit_target, split_target, 1)
        return rendered


class OracleQueueTombstoneStore(SQLSpecQueueTombstoneStore):
    """Oracle tombstone store using version-compatible plain CREATE/DROP DDL."""

    __slots__ = ()

    data_dictionary_dialect = "oracle"
    identifier_quote_style = "none"

    def create_statements(self) -> "list[str]":
        """Return a retry-safe CREATE TABLE block supported before Oracle 23c."""
        if not self._manage_schema:
            return []
        rendered = self._to_sql(self._create_tombstone_table_statement(if_not_exists=False))
        unsplit_target = self._quote_unsplit_identifier(self.table_name)
        split_target = self._quoted_table_name()
        if unsplit_target != split_target:
            rendered = rendered.replace(unsplit_target, split_target, 1)
        return [_oracle_ddl_block(rendered, ignored_code=-955)]

    def drop_statements(self) -> "list[str]":
        """Return a retry-safe DROP TABLE block supported before Oracle 23c."""
        if not self._manage_schema:
            return []
        return [_oracle_ddl_block(f"DROP TABLE {self._quoted_table_name()}", ignored_code=-942)]


class SpannerQueueTombstoneStore(SQLSpecQueueTombstoneStore):
    """Spanner tombstone store using native DDL operations and STRING/INT64 types."""

    __slots__ = ()

    data_dictionary_dialect = "spanner"
    identifier_quote_style = "backtick"
    skip_cleanup_rollback = True

    def create_statements(self) -> "list[str]":
        """Return the Spanner tombstone CREATE TABLE statement."""
        if not self._manage_schema:
            return []
        columns = (
            f"{self._quote_identifier('identity_key')} {self._indexed_text_type()} NOT NULL",
            f"{self._quote_identifier('task_id')} {self._id_type()} NOT NULL",
            f"{self._quote_identifier('task_name')} {self._indexed_text_type()} NOT NULL",
            f"{self._quote_identifier('created_at')} {self._timestamp_type()} NOT NULL",
        )
        column_sql = ",\n  ".join(columns)
        return [
            f"CREATE TABLE {self._quoted_table_name()} (\n  {column_sql}\n) "
            f"PRIMARY KEY ({self._quote_identifier('identity_key')})"
        ]

    def drop_statements(self) -> "list[str]":
        """Return the Spanner tombstone DROP TABLE statement."""
        if not self._manage_schema:
            return []
        return [f"DROP TABLE {self._quoted_table_name()}"]

    def create_schema_for_config(self, config: "Any") -> "None":
        """Create the Spanner tombstone table through the native DDL operation API.

        Returns:
            None.
        """
        if not self._manage_schema:
            return
        from litestar_queues.backends.sqlspec.stores.spanner.store import _execute_spanner_ddl

        get_database = getattr(config, "get_database", None)
        if not callable(get_database):
            msg = "Spanner tombstone schema creation requires a SQLSpec SpannerSyncConfig."
            raise TypeError(msg)
        database = get_database()
        for statement in self.create_statements():
            _execute_spanner_ddl(database, statement)

    def _string_type(self, length: "int | None" = None) -> "str":
        return "STRING(MAX)" if length is None else f"STRING({length})"

    def _integer_type(self) -> "str":
        return "INT64"

    def _timestamp_type(self) -> "str":
        return "TIMESTAMP"


class MssqlQueueTombstoneStore(SQLSpecQueueTombstoneStore):
    """SQL Server tombstone store using guarded raw T-SQL DDL.

    The generic ``sql.create_table`` builder cannot render SQL Server column
    types (``DATETIME2(6)``) through sqlglot, so the DDL is emitted as raw T-SQL
    with ``IF OBJECT_ID`` guards, mirroring the queue store's SQL Server family.
    """

    __slots__ = ()

    data_dictionary_dialect = "mssql"
    identifier_quote_style = "none"

    def create_statements(self) -> "list[str]":
        """Return the guarded SQL Server tombstone CREATE TABLE statement."""
        if not self._manage_schema:
            return []
        return [
            f"""
        IF OBJECT_ID(N'{self.table_name}', N'U') IS NULL
        BEGIN
            CREATE TABLE {self._quoted_table_name()} (
                {self._quote_identifier("identity_key")} {self._indexed_text_type()} PRIMARY KEY,
                {self._quote_identifier("task_id")} {self._id_type()} NOT NULL,
                {self._quote_identifier("task_name")} {self._indexed_text_type()} NOT NULL,
                {self._quote_identifier("created_at")} {self._timestamp_type()} NOT NULL
            );
        END;
        """
        ]

    def drop_statements(self) -> "list[str]":
        """Return the guarded SQL Server tombstone DROP TABLE statement."""
        if not self._manage_schema:
            return []
        return [f"IF OBJECT_ID(N'{self.table_name}', N'U') IS NOT NULL DROP TABLE {self._quoted_table_name()};"]

    def _string_type(self, length: "int | None" = None) -> "str":
        if length is None:
            return self._text_type()
        if length >= _NVARCHAR_MAX_THRESHOLD:
            return "NVARCHAR(MAX)"
        return f"NVARCHAR({length})"

    def _integer_type(self) -> "str":
        return "INT"

    def _quote_identifier(self, identifier: "str") -> "str":
        parts = split_qualified_identifier(identifier)
        if not parts:
            return _quote_tsql_identifier(identifier)
        return ".".join(_quote_tsql_identifier(part) for part in parts)


def resolve_uniqueness_table_name(queue_table_name: "str", *, uniqueness_table_name: "str | None" = None) -> "str":
    """Resolve the tombstone table name for a queue table.

    Returns:
        The explicit tombstone table name, or the derived queue-table tombstone name.
    """
    if uniqueness_table_name is not None:
        return validate_table_name(uniqueness_table_name)
    return uniqueness_table_name_for(queue_table_name)


def create_tombstone_store(
    config: "SQLSpecStoreConfig",
    *,
    queue_table_name: "str",
    uniqueness_table_name: "str | None" = None,
    manage_schema: "bool" = True,
) -> "SQLSpecQueueTombstoneStore":
    """Create a tombstone store for a SQLSpec adapter configuration.

    Returns:
        A tombstone store configured for the resolved tombstone table, using the
        Spanner-native store for Spanner configs and the portable store otherwise.
    """
    table_name = resolve_uniqueness_table_name(queue_table_name, uniqueness_table_name=uniqueness_table_name)
    adapter = _adapter_name(config)
    if adapter == "spanner":
        store_type: "type[SQLSpecQueueTombstoneStore]" = SpannerQueueTombstoneStore
    elif adapter == "oracledb":
        store_type = OracleQueueTombstoneStore
    elif adapter in _MSSQL_ADAPTERS:
        store_type = MssqlQueueTombstoneStore
    else:
        store_type = SQLSpecQueueTombstoneStore
    return store_type(config, table_name=table_name, manage_schema=manage_schema)


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

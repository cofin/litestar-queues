"""psycopg SQLSpec queue stores."""

from sqlspec import sql

from litestar_queues.backends.sqlspec.stores.base import SQLSpecQueueStore

__all__ = ("PsycopgAsyncQueueStore", "PsycopgSyncQueueStore")


class PsycopgSyncQueueStore(SQLSpecQueueStore):
    """psycopg sync SQLSpec queue statement store."""

    __slots__ = ()

    json_type = "JSONB"
    timestamp_type = "TIMESTAMPTZ"

    def create_statements(self) -> list[str]:
        """Return statements that create psycopg sync queue artifacts."""
        return _create_statements(self)

    def drop_statements(self) -> list[str]:
        """Return statements that drop psycopg sync queue artifacts."""
        return _drop_statements(self)

    def _create_index_statements(self) -> list[str]:
        return _create_index_statements(self)


class PsycopgAsyncQueueStore(SQLSpecQueueStore):
    """psycopg async SQLSpec queue statement store."""

    __slots__ = ()

    json_type = "JSONB"
    timestamp_type = "TIMESTAMPTZ"

    def create_statements(self) -> list[str]:
        """Return statements that create psycopg async queue artifacts."""
        return _create_statements(self)

    def drop_statements(self) -> list[str]:
        """Return statements that drop psycopg async queue artifacts."""
        return _drop_statements(self)

    def _create_index_statements(self) -> list[str]:
        return _create_index_statements(self)


def _create_statements(store: SQLSpecQueueStore) -> list[str]:
    if not store._manage_schema:
        return []
    return [
        f"{store._to_sql(store._create_table_statement())} WITH (fillfactor = 80)",
        *_create_index_statements(store),
        (
            f"ALTER TABLE {store.table_name} SET ("
            "autovacuum_vacuum_scale_factor = 0.05, "
            "autovacuum_analyze_scale_factor = 0.02)"
        ),
    ]


def _drop_statements(store: SQLSpecQueueStore) -> list[str]:
    if not store._manage_schema:
        return []
    return [
        store._to_sql(sql.drop_index(store._index_name("heartbeat")).if_exists()),
        store._to_sql(sql.drop_index(store._index_name("scheduled")).if_exists()),
        store._to_sql(sql.drop_index(store._index_name("pending")).if_exists()),
        store._to_sql(sql.drop_table(store.table_name).if_exists()),
    ]


def _create_index_statements(store: SQLSpecQueueStore) -> list[str]:
    table_name = store.table_name
    return [
        (
            f"CREATE INDEX IF NOT EXISTS {store._index_name('pending')} "
            f"ON {table_name} ({store._col('queue')}, {store._col('execution_backend')}, "
            f"{store._col('priority')} DESC, {store._col('created_at')}) "
            f"WHERE {store._col('status')} IN ('pending', 'scheduled')"
        ),
        (
            f"CREATE INDEX IF NOT EXISTS {store._index_name('scheduled')} "
            f"ON {table_name} ({store._col('scheduled_at')}) WHERE {store._col('status')} = 'scheduled'"
        ),
        (
            f"CREATE INDEX IF NOT EXISTS {store._index_name('heartbeat')} "
            f"ON {table_name} ({store._col('heartbeat_at')}) WHERE {store._col('status')} = 'running'"
        ),
    ]

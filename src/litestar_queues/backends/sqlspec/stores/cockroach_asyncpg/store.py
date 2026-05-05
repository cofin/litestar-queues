"""cockroach_asyncpg SQLSpec queue store."""

from sqlspec import sql

from litestar_queues.backends.sqlspec.stores.base import SQLSpecQueueStore

__all__ = ("CockroachAsyncpgQueueStore",)


class CockroachAsyncpgQueueStore(SQLSpecQueueStore):
    """cockroach_asyncpg-specific SQLSpec queue statement store."""

    __slots__ = ()

    json_type = "JSONB"
    timestamp_type = "TIMESTAMPTZ"

    def create_statements(self) -> list[str]:
        """Return statements that create cockroach_asyncpg queue artifacts."""
        return [
            self._to_sql(self._create_table_statement()),
            *self._create_index_statements(),
        ]

    def drop_statements(self) -> list[str]:
        """Return statements that drop cockroach_asyncpg queue artifacts."""
        return [
            self._to_sql(sql.drop_index(self._index_name("heartbeat")).if_exists()),
            self._to_sql(sql.drop_index(self._index_name("scheduled")).if_exists()),
            self._to_sql(sql.drop_index(self._index_name("pending")).if_exists()),
            self._to_sql(sql.drop_table(self.table_name).if_exists()),
        ]

    def _create_index_statements(self) -> list[str]:
        table_name = self.table_name
        return [
            (
                f"CREATE INDEX IF NOT EXISTS {self._index_name('pending')} "
                f"ON {table_name} (queue, execution_backend, priority DESC, created_at) "
                "WHERE status IN ('pending', 'scheduled')"
            ),
            (
                f"CREATE INDEX IF NOT EXISTS {self._index_name('scheduled')} "
                f"ON {table_name} (scheduled_at) WHERE status = 'scheduled'"
            ),
            (
                f"CREATE INDEX IF NOT EXISTS {self._index_name('heartbeat')} "
                f"ON {table_name} (heartbeat_at) WHERE status = 'running'"
            ),
        ]

"""psqlpy SQLSpec queue store."""

from typing import Any

from sqlspec import sql

from litestar_queues.backends.sqlspec.stores.base import SQLSpecQueueStore

__all__ = ("PsqlpyQueueStore",)


class PsqlpyQueueStore(SQLSpecQueueStore):
    """psqlpy-specific SQLSpec queue statement store."""

    __slots__ = ()

    json_type = "JSONB"
    result_json_type = "TEXT"
    timestamp_type = "TIMESTAMPTZ"

    def __init__(self, config: Any, *, native_json_columns: frozenset[str] | None = None, **kwargs: Any) -> None:
        super().__init__(
            config,
            native_json_columns=native_json_columns or frozenset({"args_json", "kwargs_json", "metadata_json"}),
            **kwargs,
        )

    def create_statements(self) -> list[str]:
        """Return statements that create psqlpy queue artifacts."""
        if not self._manage_schema:
            return []
        return [
            f"{self._to_sql(self._create_table_statement())} WITH (fillfactor = 80)",
            *self._create_index_statements(),
            (
                f"ALTER TABLE {self.table_name} SET ("
                "autovacuum_vacuum_scale_factor = 0.05, "
                "autovacuum_analyze_scale_factor = 0.02)"
            ),
        ]

    def drop_statements(self) -> list[str]:
        """Return statements that drop psqlpy queue artifacts."""
        if not self._manage_schema:
            return []
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
                f"ON {table_name} ({self._col('queue')}, {self._col('execution_backend')}, "
                f"{self._col('priority')} DESC, {self._col('created_at')}) "
                f"WHERE {self._col('status')} IN ('pending', 'scheduled')"
            ),
            (
                f"CREATE INDEX IF NOT EXISTS {self._index_name('scheduled')} "
                f"ON {table_name} ({self._col('scheduled_at')}) WHERE {self._col('status')} = 'scheduled'"
            ),
            (
                f"CREATE INDEX IF NOT EXISTS {self._index_name('heartbeat')} "
                f"ON {table_name} ({self._col('heartbeat_at')}) WHERE {self._col('status')} = 'running'"
            ),
        ]

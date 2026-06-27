"""psqlpy SQLSpec queue store."""

from typing import Any

from litestar_queues.backends.sqlspec.stores._families import PostgresQueueStore

__all__ = ("PsqlpyQueueStore",)


class PsqlpyQueueStore(PostgresQueueStore):
    """psqlpy-specific SQLSpec queue statement store."""

    __slots__ = ()

    auto_native_json_columns = frozenset()
    table_storage_parameters = True

    def __init__(self, config: Any, *, native_json_columns: frozenset[str] | None = None, **kwargs: Any) -> None:
        super().__init__(
            config,
            native_json_columns=native_json_columns or frozenset({"args_json", "kwargs_json", "metadata_json"}),
            **kwargs,
        )

    def _result_json_type(self, column_name: str) -> str:
        del column_name
        return self._text_type()

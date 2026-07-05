"""Internal typing helpers for the SQLSpec queue backend."""

from datetime import datetime
from typing import TYPE_CHECKING, Any, Protocol, TypeAlias

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Sequence


DatetimeParam: TypeAlias = datetime | str


class SQLSpecConfig(Protocol):
    """Structural subset used from SQLSpec adapter configs."""

    statement_config: Any
    extension_config: Any
    migration_config: Any

    @property
    def is_async(self) -> bool: ...

    def close_pool(self) -> Any: ...

    def get_migration_commands(self) -> Any: ...

    def get_observability_runtime(self) -> Any: ...

    def migrate_up(self, *args: Any, **kwargs: Any) -> Any: ...

    def set_migration_config(self, config: Any) -> None: ...


class SQLSpecStoreConfig(Protocol):
    """Structural subset needed by queue stores and the store factory."""

    statement_config: Any
    extension_config: Any


class SQLSpecSessionConfig(Protocol):
    """Structural subset needed to obtain a SQLSpec session."""

    @property
    def is_async(self) -> bool: ...


class SQLSpecManager(Protocol):
    """Structural subset used from ``sqlspec.SQLSpec``."""

    def provide_session(self, config: Any) -> Any: ...


class SQLSpecDriver(Protocol):
    """Awaitable driver surface used by the queue backend."""

    async def begin(self) -> Any: ...

    async def commit(self) -> Any: ...

    async def rollback(self) -> Any: ...

    async def execute(self, statement: Any, *parameters: Any, **kwargs: Any) -> Any: ...

    async def execute_many(self, statement: Any, parameters: "Sequence[dict[str, Any]]") -> Any: ...

    async def execute_script(self, statement: str) -> Any: ...

    async def load_from_records(self, table_name: str, records: "Sequence[dict[str, Any]]") -> Any: ...

    async def select(self, statement: Any, *parameters: Any, **kwargs: Any) -> list[Any]: ...

    async def select_one_or_none(self, statement: Any, *parameters: Any, **kwargs: Any) -> Any | None: ...

    def select_stream(self, statement: Any, *, chunk_size: int | None = None) -> "AsyncIterator[Any] | Any": ...

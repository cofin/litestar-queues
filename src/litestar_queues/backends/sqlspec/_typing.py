"""Internal typing helpers for the SQLSpec queue backend."""

from datetime import datetime
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Protocol, TypeAlias, cast

from litestar_queues.exceptions import QueueConfigurationError

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Sequence
    from pathlib import Path

    from sqlspec.adapters.asyncpg import AsyncpgConfig
    from sqlspec.adapters.sqlite import SqliteConfig

__all__ = (
    "DEFAULT_MIGRATION_MODE",
    "VALID_MIGRATION_MODES",
    "DatetimeParam",
    "ExtensionMigrationMode",
    "SQLSpecConfig",
    "SQLSpecDriver",
    "SQLSpecManager",
    "SQLSpecSessionConfig",
    "SQLSpecStoreConfig",
    "validate_migration_mode",
)


class SQLSpecConfig(Protocol):
    """Structural subset used from SQLSpec adapter configs."""

    statement_config: Any
    extension_config: Any
    migration_config: Any
    is_async: ClassVar[bool]
    supports_reliable_rowcount: ClassVar[bool]

    def add_extension_migrations(
        self,
        name: str,
        migrations_path: "str | Path",
        settings: "dict[str, Any] | None" = None,
        *,
        migration_mode: "ExtensionMigrationMode | None" = None,
    ) -> None: ...

    def remove_extension_migrations(self, name: str) -> bool: ...

    def close_pool(self) -> Any: ...

    def get_migration_commands(self) -> Any: ...

    def get_observability_runtime(self) -> Any: ...

    def migrate_up(self, *args: Any, **kwargs: Any) -> Any: ...

    def set_migration_config(self, config: Any) -> None: ...


class SQLSpecStoreConfig(Protocol):
    """Structural subset needed by queue stores and the store factory."""

    is_async: ClassVar[bool]
    statement_config: Any
    extension_config: Any


class SQLSpecSessionConfig(Protocol):
    """Structural subset needed to obtain a SQLSpec session."""

    is_async: ClassVar[bool]


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


if TYPE_CHECKING:
    _sync_config_contract: SQLSpecConfig = SqliteConfig()
    _async_config_contract: SQLSpecConfig = AsyncpgConfig()
    _sync_session_contract: SQLSpecSessionConfig = SqliteConfig()
    _async_session_contract: SQLSpecSessionConfig = AsyncpgConfig()


DatetimeParam: TypeAlias = datetime | str
ExtensionMigrationMode: TypeAlias = Literal["versioned", "reconcile"]
VALID_MIGRATION_MODES: "frozenset[str]" = frozenset({"versioned", "reconcile"})
DEFAULT_MIGRATION_MODE: "ExtensionMigrationMode" = "reconcile"


def validate_migration_mode(migration_mode: "object") -> "ExtensionMigrationMode":
    """Validate SQLSpec extension migration mode and reject booleans or unknown strings."""
    if (
        isinstance(migration_mode, bool)
        or not isinstance(migration_mode, str)
        or migration_mode not in VALID_MIGRATION_MODES
    ):
        valid = ", ".join(sorted(VALID_MIGRATION_MODES))
        msg = f"Invalid SQLSpec migration_mode {migration_mode!r}; expected one of: {valid}."
        raise QueueConfigurationError(msg)
    return cast("ExtensionMigrationMode", migration_mode)

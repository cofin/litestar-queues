"""Private typing helpers for the optional SQLSpec backend."""

from typing import TYPE_CHECKING, Any

from litestar_queues.exceptions import MissingDependencyError

if TYPE_CHECKING:
    from sqlspec import SQLSpec
    from sqlspec.config import AsyncDatabaseConfig, NoPoolAsyncConfig
    from sqlspec.extensions.events import AsyncEventChannel

    AsyncEventChannelT = AsyncEventChannel
    SQLSpecT = SQLSpec
    SQLSpecConfigT = AsyncDatabaseConfig[Any, Any, Any] | NoPoolAsyncConfig[Any, Any]
else:
    AsyncEventChannelT = Any
    SQLSpecT = Any
    SQLSpecConfigT = Any


def sqlspec_installed() -> bool:
    """Return whether SQLSpec can be imported."""
    try:
        import sqlspec  # noqa: F401
    except ModuleNotFoundError:
        return False
    return True


def missing_sqlspec_error(exc: ModuleNotFoundError) -> MissingDependencyError:
    """Build a package-specific missing SQLSpec dependency error.

    Returns:
        A queue missing dependency error for SQLSpec.
    """
    return MissingDependencyError(exc.name or "sqlspec", "sqlspec")

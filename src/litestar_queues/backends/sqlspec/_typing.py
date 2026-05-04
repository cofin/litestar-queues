"""Private typing helpers for the optional SQLSpec backend."""

from typing import Any, Protocol, runtime_checkable

from litestar_queues.exceptions import MissingDependencyError

SQLSpecT = Any
SQLSpecConfigT = Any
SQLFileLoaderT = Any


def sqlspec_installed() -> bool:
    """Return whether SQLSpec can be imported."""
    try:
        import sqlspec  # noqa: F401
    except ModuleNotFoundError:
        return False
    return True


def missing_sqlspec_error(exc: ModuleNotFoundError) -> MissingDependencyError:
    """Build a package-specific missing SQLSpec dependency error."""
    return MissingDependencyError(exc.name or "sqlspec", "sqlspec")


@runtime_checkable
class SQLSpecPluginProtocol(Protocol):
    """Protocol for SQLSpec Litestar plugin objects."""

    def on_app_init(self, app_config: Any) -> Any:
        """Apply SQLSpec integration to a Litestar app config."""


SQLSpecPluginT = SQLSpecPluginProtocol

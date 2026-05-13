"""Private typing helpers for the optional SQLSpec backend."""

from typing import TYPE_CHECKING, Any

from litestar_queues.exceptions import MissingDependencyError

if TYPE_CHECKING:
    from sqlspec import SQLSpec
    from sqlspec.extensions.events import AsyncEventChannel

    AsyncEventChannelT = AsyncEventChannel
    SQLSpecT = SQLSpec
    # ``SQLSpecQueueBackend`` accepts both async and sync SQLSpec configs;
    # sync configs are bridged through ``sqlspec.utils.sync_tools`` inside
    # ``_bridge_session``. Typing as the explicit ``Async | Sync`` union pulls
    # ambiguous overload variants of ``provide_session`` and
    # ``AsyncEventChannel`` methods into pyright's view, so we widen to ``Any``
    # at the alias boundary.
    SQLSpecConfigT = Any
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

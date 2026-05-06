"""Optional dependency helpers for the Advanced Alchemy queue backend."""

from typing import Any

from litestar_queues.exceptions import MissingDependencyError

__all__ = (
    "AsyncSessionMakerT",
    "AsyncSessionT",
    "SQLAlchemyAsyncConfigT",
    "advanced_alchemy_installed",
    "missing_advanced_alchemy_error",
)

SQLAlchemyAsyncConfigT = Any
AsyncSessionMakerT = Any
AsyncSessionT = Any


def advanced_alchemy_installed() -> bool:
    """Return whether the Advanced Alchemy optional dependency is importable."""
    try:
        import advanced_alchemy  # noqa: F401
        import sqlalchemy  # noqa: F401
    except ModuleNotFoundError:
        return False
    return True


def missing_advanced_alchemy_error(exc: ModuleNotFoundError) -> MissingDependencyError:
    """Return the backend's optional dependency error."""
    missing_name = exc.name or "advanced-alchemy"
    install_extra = "advanced-alchemy"
    return MissingDependencyError(missing_name, install_extra)

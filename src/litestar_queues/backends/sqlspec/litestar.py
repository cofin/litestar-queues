"""Litestar integration helpers for the SQLSpec queue backend."""

from typing import Any

from litestar_queues.backends.sqlspec._typing import missing_sqlspec_error

__all__ = ("build_sqlspec_plugin",)


def build_sqlspec_plugin(sqlspec: Any, loader: Any | None = None) -> Any:
    """Build SQLSpec's first-party Litestar plugin."""
    try:
        from sqlspec.extensions.litestar import SQLSpecPlugin
    except ModuleNotFoundError as exc:
        raise missing_sqlspec_error(exc) from exc
    return SQLSpecPlugin(sqlspec, loader=loader)

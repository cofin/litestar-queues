"""Correlation ID propagation across the queue boundary.

SQLSpec's framework middleware derives a correlation ID from request headers and
holds it in a context variable. A task enqueued during that request runs later,
in a different process, with that context long gone -- so the ID is carried on
the queue record and rebound for the duration of execution.

This module deliberately imports neither SQLSpec nor any telemetry package at
module scope: ``QueueService`` imports it directly, and core queue APIs must stay
importable without the optional extras.
"""

from functools import lru_cache
from importlib import import_module
from importlib.util import find_spec
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Mapping

__all__ = (
    "CORRELATION_ID_METADATA_KEY",
    "SQLSPEC_INSTALLED",
    "bind_correlation_id",
    "capture_correlation_id",
    "reset_correlation_id",
    "sqlspec_correlation_context",
)

SQLSPEC_INSTALLED = find_spec("sqlspec") is not None

CORRELATION_ID_METADATA_KEY = "_correlation_id"
"""Reserved metadata key carrying the enqueueing request's correlation ID."""


@lru_cache(maxsize=1)
def sqlspec_correlation_context() -> "Any | None":
    """Return SQLSpec's ``CorrelationContext``.

    Resolved lazily rather than at import time, because importing SQLSpec is not
    free and most consumers of this module never need it.

    Returns:
        SQLSpec's ``CorrelationContext`` class, or ``None`` when SQLSpec is absent.
    """
    if not SQLSPEC_INSTALLED:
        return None
    try:
        return import_module("sqlspec.utils.correlation").CorrelationContext
    except ImportError:  # pragma: no cover - guards a partial SQLSpec install
        return None


def capture_correlation_id(metadata: "dict[str, Any]") -> "None":
    """Store the currently active correlation ID on a queued record."""
    correlation_context = sqlspec_correlation_context()
    if correlation_context is None:
        return
    correlation_id = correlation_context.get()
    if correlation_id:
        metadata[CORRELATION_ID_METADATA_KEY] = correlation_id


def bind_correlation_id(metadata: "Mapping[str, Any]") -> "tuple[Any, bool]":
    """Rebind the enqueueing request's correlation ID for task execution.

    Returns:
        The previously active correlation ID, and whether it must be restored.
    """
    correlation_context = sqlspec_correlation_context()
    if correlation_context is None:
        return None, False
    correlation_id = metadata.get(CORRELATION_ID_METADATA_KEY)
    if not isinstance(correlation_id, str) or not correlation_id:
        return None, False
    previous = correlation_context.get()
    correlation_context.set(correlation_id)
    return previous, True


def reset_correlation_id(state: "tuple[Any, bool]") -> "None":
    """Restore the correlation ID that was active before task execution."""
    previous, bound = state
    if not bound:
        return
    correlation_context = sqlspec_correlation_context()
    if correlation_context is not None:
        correlation_context.set(previous)

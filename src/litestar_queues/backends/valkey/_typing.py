"""Optional Valkey dependency helpers."""

from typing import Any

from litestar_queues.exceptions import MissingDependencyError

__all__ = ("create_valkey_client", "missing_valkey_error")


def missing_valkey_error(exc: ModuleNotFoundError) -> MissingDependencyError:
    """Return a standard optional dependency error for Valkey."""
    return MissingDependencyError(exc.name or "valkey", "valkey")


def create_valkey_client(url: str) -> Any:
    """Create a Valkey asyncio client from a URL.

    Returns:
        A Valkey asyncio client.
    """
    try:
        from valkey import asyncio as valkey_asyncio
    except ModuleNotFoundError as exc:
        error = missing_valkey_error(exc)
        raise error from exc
    return valkey_asyncio.from_url(url, decode_responses=True)

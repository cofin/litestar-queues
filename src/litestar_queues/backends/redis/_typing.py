"""Optional Redis dependency helpers."""

from typing import Any

from litestar_queues.exceptions import MissingDependencyError

__all__ = ("create_redis_client", "missing_redis_error")


def missing_redis_error(exc: ModuleNotFoundError) -> MissingDependencyError:
    """Return a standard optional dependency error for Redis."""
    return MissingDependencyError(exc.name or "redis", "redis")


def create_redis_client(url: str) -> Any:
    """Create a Redis asyncio client from a URL.

    Returns:
        A Redis asyncio client.
    """
    try:
        from redis import asyncio as redis_asyncio
    except ModuleNotFoundError as exc:
        error = missing_redis_error(exc)
        raise error from exc
    return redis_asyncio.from_url(url, decode_responses=True)

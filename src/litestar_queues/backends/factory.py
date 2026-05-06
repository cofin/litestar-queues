"""Queue backend registry and factory functions."""

from collections.abc import Callable
from functools import lru_cache
from importlib import import_module
from inspect import signature
from typing import TYPE_CHECKING, Any

from litestar_queues.backends.base import BaseQueueBackend

if TYPE_CHECKING:
    from litestar_queues.config import QueueConfig

__all__ = (
    "get_queue_backend",
    "get_queue_backend_class",
    "list_queue_backends",
    "queue_backend",
)

_queue_backend_registry: dict[str, type[BaseQueueBackend]] = {}


def queue_backend(name: str) -> Callable[[type[BaseQueueBackend]], type[BaseQueueBackend]]:
    """Decorator to register a queue backend class with a short name.

    Returns:
        A decorator that registers the backend class.
    """

    def decorator(cls: type[BaseQueueBackend]) -> type[BaseQueueBackend]:
        _queue_backend_registry[name] = cls
        return cls

    return decorator


@lru_cache(maxsize=1)
def _register_builtins() -> None:
    """Register built-in queue backends lazily."""
    from litestar_queues.backends.advanced_alchemy import AdvancedAlchemyQueueBackend
    from litestar_queues.backends.memory import InMemoryQueueBackend
    from litestar_queues.backends.redis import RedisQueueBackend
    from litestar_queues.backends.sqlspec import SQLSpecQueueBackend
    from litestar_queues.backends.valkey import ValkeyQueueBackend

    _queue_backend_registry.setdefault("advanced-alchemy", AdvancedAlchemyQueueBackend)
    _queue_backend_registry.setdefault("memory", InMemoryQueueBackend)
    _queue_backend_registry.setdefault("redis", RedisQueueBackend)
    _queue_backend_registry.setdefault("sqlspec", SQLSpecQueueBackend)
    _queue_backend_registry.setdefault("valkey", ValkeyQueueBackend)


def get_queue_backend_class(backend_path: str) -> type[BaseQueueBackend]:
    """Get a queue backend class by short name or import path.

    Returns:
        The resolved queue backend class.

    Raises:
        ValueError: If a short backend name is unknown.
    """
    _register_builtins()

    if backend_path in _queue_backend_registry:
        return _queue_backend_registry[backend_path]

    if "." not in backend_path:
        msg = f"Unknown queue backend: {backend_path!r}. Available: {list(_queue_backend_registry.keys())}"
        raise ValueError(msg)

    module_path, class_name = backend_path.rsplit(".", 1)
    module = import_module(module_path)
    return getattr(module, class_name)  # type: ignore[no-any-return]


def get_queue_backend(backend: str = "memory", config: "QueueConfig | None" = None) -> BaseQueueBackend:
    """Get an instantiated queue backend.

    Returns:
        A configured queue backend instance.
    """
    backend_class = get_queue_backend_class(backend)
    backend_kwargs: dict[str, Any] = {"config": config}
    if config is not None:
        backend_kwargs.update(config.queue_backend_config)

    init_signature = signature(backend_class.__init__)
    accepts_kwargs = any(param.kind == param.VAR_KEYWORD for param in init_signature.parameters.values())
    if not accepts_kwargs:
        backend_kwargs = {key: value for key, value in backend_kwargs.items() if key in init_signature.parameters}

    return backend_class(**backend_kwargs)


def list_queue_backends() -> list[str]:
    """Return registered queue backend names."""
    _register_builtins()
    return list(_queue_backend_registry.keys())

"""Storage backend registry and factory functions."""

from collections.abc import Callable
from functools import lru_cache
from importlib import import_module
from inspect import signature
from typing import TYPE_CHECKING, Any

from litestar_queues.backends.base import BaseStorageBackend

if TYPE_CHECKING:
    from litestar_queues.config import QueueConfig

__all__ = (
    "get_storage_backend",
    "get_storage_backend_class",
    "list_storage_backends",
    "storage_backend",
)

_storage_backend_registry: dict[str, type[BaseStorageBackend]] = {}


def storage_backend(name: str) -> Callable[[type[BaseStorageBackend]], type[BaseStorageBackend]]:
    """Decorator to register a storage backend class with a short name.

    Returns:
        A decorator that registers the backend class.
    """

    def decorator(cls: type[BaseStorageBackend]) -> type[BaseStorageBackend]:
        _storage_backend_registry[name] = cls
        return cls

    return decorator


@lru_cache(maxsize=1)
def _register_builtins() -> None:
    """Register built-in storage backends lazily."""
    from litestar_queues.backends.memory import InMemoryStorageBackend
    from litestar_queues.backends.sqlspec import SQLSpecStorageBackend

    _storage_backend_registry.setdefault("memory", InMemoryStorageBackend)
    _storage_backend_registry.setdefault("sqlspec", SQLSpecStorageBackend)


def get_storage_backend_class(backend_path: str) -> type[BaseStorageBackend]:
    """Get a storage backend class by short name or import path.

    Returns:
        The resolved storage backend class.

    Raises:
        ValueError: If a short backend name is unknown.
    """
    _register_builtins()

    if backend_path in _storage_backend_registry:
        return _storage_backend_registry[backend_path]

    if "." not in backend_path:
        msg = f"Unknown storage backend: {backend_path!r}. Available: {list(_storage_backend_registry.keys())}"
        raise ValueError(msg)

    module_path, class_name = backend_path.rsplit(".", 1)
    module = import_module(module_path)
    return getattr(module, class_name)  # type: ignore[no-any-return]


def get_storage_backend(backend: str = "memory", config: "QueueConfig | None" = None) -> BaseStorageBackend:
    """Get an instantiated storage backend.

    Returns:
        A configured storage backend instance.
    """
    backend_class = get_storage_backend_class(backend)
    backend_kwargs: dict[str, Any] = {"config": config}
    if config is not None:
        backend_kwargs.update(config.storage_backend_config)

    init_signature = signature(backend_class.__init__)
    accepts_kwargs = any(param.kind == param.VAR_KEYWORD for param in init_signature.parameters.values())
    if not accepts_kwargs:
        backend_kwargs = {key: value for key, value in backend_kwargs.items() if key in init_signature.parameters}

    return backend_class(**backend_kwargs)


def list_storage_backends() -> list[str]:
    """Return registered storage backend names."""
    _register_builtins()
    return list(_storage_backend_registry.keys())

"""Queue backend registry and factory functions."""

from collections.abc import Callable
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

_BUILTIN_BACKENDS: dict[str, str] = {
    "advanced-alchemy": "litestar_queues.backends.advanced_alchemy:AdvancedAlchemyQueueBackend",
    "memory": "litestar_queues.backends.memory:InMemoryQueueBackend",
    "redis": "litestar_queues.backends.redis:RedisQueueBackend",
    "sqlspec": "litestar_queues.backends.sqlspec:SQLSpecQueueBackend",
    "valkey": "litestar_queues.backends.valkey:ValkeyQueueBackend",
}


def queue_backend(name: str) -> Callable[[type[BaseQueueBackend]], type[BaseQueueBackend]]:
    """Decorator to register a queue backend class with a short name.

    Returns:
        A decorator that registers the backend class.
    """

    def decorator(cls: type[BaseQueueBackend]) -> type[BaseQueueBackend]:
        _queue_backend_registry[name] = cls
        return cls

    return decorator


def get_queue_backend_class(backend_path: str) -> type[BaseQueueBackend]:
    """Get a queue backend class by short name or import path.

    Optional backends are imported lazily on first lookup so unused adapters do
    not require their driver extras to be installed.

    Returns:
        The resolved queue backend class.

    Raises:
        ValueError: If a short backend name is unknown.
    """
    if backend_path in _queue_backend_registry:
        return _queue_backend_registry[backend_path]

    if backend_path in _BUILTIN_BACKENDS:
        module_path, class_name = _BUILTIN_BACKENDS[backend_path].split(":", 1)
        module = import_module(module_path)
        backend_class = cast_backend_class(getattr(module, class_name))
        _queue_backend_registry[backend_path] = backend_class
        return backend_class

    if "." not in backend_path:
        available = sorted({*_queue_backend_registry, *_BUILTIN_BACKENDS})
        msg = f"Unknown queue backend: {backend_path!r}. Available: {available}"
        raise ValueError(msg)

    module_path, class_name = backend_path.rsplit(".", 1)
    module = import_module(module_path)
    return cast_backend_class(getattr(module, class_name))


def cast_backend_class(value: Any) -> type[BaseQueueBackend]:
    """Narrow an imported attribute to the backend class type for type-checkers.

    Returns:
        The same value, typed as ``type[BaseQueueBackend]``.
    """
    return value  # type: ignore[no-any-return]


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
    """Return registered queue backend names (built-ins + dynamically registered)."""
    return sorted({*_queue_backend_registry, *_BUILTIN_BACKENDS})

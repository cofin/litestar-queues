"""Queue backend registry and factory functions."""

from importlib import import_module
from inspect import signature
from typing import TYPE_CHECKING, Any, cast

from litestar_queues.config import QueueBackendConfig, QueueConfig, queue_backend_name

if TYPE_CHECKING:
    from collections.abc import Callable

    from litestar_queues.backends.base import BaseQueueBackend

__all__ = ("get_queue_backend", "get_queue_backend_class", "list_queue_backends", "queue_backend")

_queue_backend_registry: "dict[str, type[BaseQueueBackend]]" = {}

_BUILTIN_BACKENDS: "dict[str, str]" = {
    "advanced-alchemy": "litestar_queues.backends.advanced_alchemy:AdvancedAlchemyQueueBackend",
    "memory": "litestar_queues.backends.memory:InMemoryQueueBackend",
    "redis": "litestar_queues.backends.redis:RedisQueueBackend",
    "sqlspec": "litestar_queues.backends.sqlspec:SQLSpecQueueBackend",
    "valkey": "litestar_queues.backends.valkey:ValkeyQueueBackend",
}


def queue_backend(name: "str") -> "Callable[[type[BaseQueueBackend]], type[BaseQueueBackend]]":
    """Decorator to register a queue backend class with a short name.

    Returns:
        A decorator that registers the backend class.
    """

    def decorator(cls: "type[BaseQueueBackend]") -> "type[BaseQueueBackend]":
        _queue_backend_registry[name] = cls
        return cls

    return decorator


def get_queue_backend_class(backend_path: "str") -> "type[BaseQueueBackend]":
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
        backend_class = _backend_class(getattr(module, class_name))
        _queue_backend_registry[backend_path] = backend_class
        return backend_class

    if "." not in backend_path:
        available = sorted({*_queue_backend_registry, *_BUILTIN_BACKENDS})
        msg = f"Unknown queue backend: {backend_path!r}. Available: {available}"
        raise ValueError(msg)

    module_path, class_name = backend_path.rsplit(".", 1)
    module = import_module(module_path)
    return _backend_class(getattr(module, class_name))


def get_queue_backend(
    backend: "QueueBackendConfig" = "memory", config: "QueueConfig | None" = None
) -> "BaseQueueBackend":
    """Get an instantiated queue backend.

    Returns:
        A configured queue backend instance.

    Raises:
        TypeError: If a typed backend config selects a backend class that does
            not accept ``backend_config``.
    """
    backend_config = None if isinstance(backend, str) else backend
    backend_class = get_queue_backend_class(queue_backend_name(backend))
    backend_kwargs: "dict[str, Any]" = {"config": config}
    if backend_config is not None:
        backend_kwargs["backend_config"] = backend_config

    init_signature = signature(backend_class.__init__)
    accepts_kwargs = any(param.kind == param.VAR_KEYWORD for param in init_signature.parameters.values())
    if backend_config is not None and not accepts_kwargs and "backend_config" not in init_signature.parameters:
        msg = f"{backend_class.__name__} must accept backend_config when selected by a typed backend config."
        raise TypeError(msg)
    if not accepts_kwargs:
        backend_kwargs = {key: value for key, value in backend_kwargs.items() if key in init_signature.parameters}

    return backend_class(**backend_kwargs)


def list_queue_backends() -> "list[str]":
    """Return registered queue backend names (built-ins + dynamically registered)."""
    return sorted({*_queue_backend_registry, *_BUILTIN_BACKENDS})


def _backend_class(value: "Any") -> "type[BaseQueueBackend]":
    return cast("type[BaseQueueBackend]", value)

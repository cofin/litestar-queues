"""Execution backend registry and factory functions."""

from collections.abc import Callable
from functools import lru_cache
from importlib import import_module
from inspect import signature
from typing import TYPE_CHECKING, Any

from litestar_queues.execution.base import BaseExecutionBackend

if TYPE_CHECKING:
    from litestar_queues.config import ExecutionBackendConfig, QueueConfig

__all__ = (
    "execution_backend",
    "get_execution_backend",
    "get_execution_backend_class",
    "list_execution_backends",
)

_execution_backend_registry: dict[str, type[BaseExecutionBackend]] = {}


def execution_backend(name: str) -> Callable[[type[BaseExecutionBackend]], type[BaseExecutionBackend]]:
    """Decorator to register an execution backend class with a short name.

    Returns:
        A decorator that registers the backend class.
    """

    def decorator(cls: type[BaseExecutionBackend]) -> type[BaseExecutionBackend]:
        _execution_backend_registry[name] = cls
        return cls

    return decorator


@lru_cache(maxsize=1)
def _register_builtins() -> None:
    """Register built-in execution backends lazily."""
    from litestar_queues.execution.cloudrun import CloudRunExecutionBackend
    from litestar_queues.execution.immediate import ImmediateExecutionBackend
    from litestar_queues.execution.local import LocalExecutionBackend

    _execution_backend_registry.setdefault("cloudrun", CloudRunExecutionBackend)
    _execution_backend_registry.setdefault("immediate", ImmediateExecutionBackend)
    _execution_backend_registry.setdefault("local", LocalExecutionBackend)


def get_execution_backend_class(backend_path: str) -> type[BaseExecutionBackend]:
    """Get an execution backend class by short name or import path.

    Returns:
        The resolved execution backend class.

    Raises:
        ValueError: If a short backend name is unknown.
    """
    _register_builtins()

    if backend_path in _execution_backend_registry:
        return _execution_backend_registry[backend_path]

    if "." not in backend_path:
        msg = f"Unknown execution backend: {backend_path!r}. Available: {list(_execution_backend_registry.keys())}"
        raise ValueError(msg)

    module_path, class_name = backend_path.rsplit(".", 1)
    module = import_module(module_path)
    return getattr(module, class_name)  # type: ignore[no-any-return]


def get_execution_backend(
    backend: "ExecutionBackendConfig" = "immediate",
    config: "QueueConfig | None" = None,
) -> BaseExecutionBackend:
    """Get an instantiated execution backend.

    Returns:
        A configured execution backend instance.

    Raises:
        TypeError: If a typed execution config selects a backend class that
            does not accept ``execution_config``.
    """
    from litestar_queues.config import execution_backend_name

    execution_config = None if isinstance(backend, str) else backend
    backend_class = get_execution_backend_class(execution_backend_name(backend))
    backend_kwargs: dict[str, Any] = {"config": config}
    if execution_config is not None:
        backend_kwargs["execution_config"] = execution_config

    init_signature = signature(backend_class.__init__)
    accepts_kwargs = any(param.kind == param.VAR_KEYWORD for param in init_signature.parameters.values())
    if execution_config is not None and not accepts_kwargs and "execution_config" not in init_signature.parameters:
        msg = f"{backend_class.__name__} must accept execution_config when selected by a typed execution config."
        raise TypeError(msg)
    if not accepts_kwargs:
        backend_kwargs = {key: value for key, value in backend_kwargs.items() if key in init_signature.parameters}

    return backend_class(**backend_kwargs)


def list_execution_backends() -> list[str]:
    """Return registered execution backend names."""
    _register_builtins()
    return list(_execution_backend_registry.keys())

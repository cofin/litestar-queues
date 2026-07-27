"""Queue execution backends public re-exports.

The Cloud Run and Cloud Tasks backends are loaded lazily, mirroring
:mod:`litestar_queues.backends`: selecting local or immediate execution never
imports an optional adapter or requires its extra to be installed.
"""

from importlib import import_module
from typing import TYPE_CHECKING, Any

from litestar_queues.execution.base import BaseExecutionBackend
from litestar_queues.execution.factory import (
    execution_backend,
    get_execution_backend,
    get_execution_backend_class,
    list_execution_backends,
)
from litestar_queues.execution.immediate import ImmediateExecutionBackend
from litestar_queues.execution.local import LocalExecutionBackend

if TYPE_CHECKING:
    from litestar_queues.execution.cloudrun import (
        CloudRunExecutionBackend,
        CloudRunExecutionConfig,
        CloudRunExecutionStatus,
    )
    from litestar_queues.execution.cloudtasks import CloudTasksExecutionBackend, CloudTasksExecutionConfig

__all__ = (
    "BaseExecutionBackend",
    "CloudRunExecutionBackend",
    "CloudRunExecutionConfig",
    "CloudRunExecutionStatus",
    "CloudTasksExecutionBackend",
    "CloudTasksExecutionConfig",
    "ImmediateExecutionBackend",
    "LocalExecutionBackend",
    "execution_backend",
    "get_execution_backend",
    "get_execution_backend_class",
    "list_execution_backends",
)

_LAZY_EXPORTS: "dict[str, str]" = {
    "CloudRunExecutionBackend": "cloudrun",
    "CloudRunExecutionConfig": "cloudrun",
    "CloudRunExecutionStatus": "cloudrun",
    "CloudTasksExecutionBackend": "cloudtasks",
    "CloudTasksExecutionConfig": "cloudtasks",
}


def __getattr__(name: "str") -> "Any":
    """Lazily load an optional execution backend export.

    Returns:
        The requested execution export.

    Raises:
        AttributeError: If the name is not an execution export.
    """
    submodule = _LAZY_EXPORTS.get(name)
    if submodule is not None:
        value = getattr(import_module(f"litestar_queues.execution.{submodule}"), name)
        globals()[name] = value
        return value
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)

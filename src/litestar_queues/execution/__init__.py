"""Queue execution backends public re-exports.

The Cloud Run backend is loaded lazily, mirroring
:mod:`litestar_queues.backends`: selecting local or immediate execution never
imports an optional adapter or requires its extra to be installed.
"""

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

__all__ = (
    "BaseExecutionBackend",
    "CloudRunExecutionBackend",
    "CloudRunExecutionConfig",
    "CloudRunExecutionStatus",
    "ImmediateExecutionBackend",
    "LocalExecutionBackend",
    "execution_backend",
    "get_execution_backend",
    "get_execution_backend_class",
    "list_execution_backends",
)

_CLOUDRUN_EXPORTS = frozenset({"CloudRunExecutionBackend", "CloudRunExecutionConfig", "CloudRunExecutionStatus"})


def __getattr__(name: "str") -> "Any":
    """Lazily load the optional Cloud Run execution backend.

    Returns:
        The requested Cloud Run export.

    Raises:
        AttributeError: If the name is not an execution export.
    """
    if name in _CLOUDRUN_EXPORTS:
        from litestar_queues.execution import cloudrun

        value = getattr(cloudrun, name)
        globals()[name] = value
        return value
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)

from litestar_queues.execution.base import BaseExecutionBackend

__all__ = ("ImmediateExecutionBackend",)


class ImmediateExecutionBackend(BaseExecutionBackend):
    """Immediate execution backend placeholder."""

    __slots__ = ()

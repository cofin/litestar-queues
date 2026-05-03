from litestar_queues.execution.base import BaseExecutionBackend

__all__ = ("LocalExecutionBackend",)


class LocalExecutionBackend(BaseExecutionBackend):
    """Local worker execution backend placeholder."""

    __slots__ = ()

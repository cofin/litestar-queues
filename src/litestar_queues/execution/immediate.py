from typing import TYPE_CHECKING

from litestar_queues.execution.base import BaseExecutionBackend

if TYPE_CHECKING:
    from litestar_queues.models import QueuedTaskRecord
    from litestar_queues.service import QueueService

__all__ = ("ImmediateExecutionBackend",)


class ImmediateExecutionBackend(BaseExecutionBackend):
    """Execution backend that runs records inline."""

    __slots__ = ()

    async def execute(self, service: "QueueService", record: "QueuedTaskRecord") -> "QueuedTaskRecord":
        """Execute a task immediately in the current event loop.

        Returns:
            The updated queue record.
        """
        return await service.execute_record(record)

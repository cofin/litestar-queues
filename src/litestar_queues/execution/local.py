from typing import TYPE_CHECKING

from litestar_queues.execution.base import BaseExecutionBackend

if TYPE_CHECKING:
    from litestar_queues.models import QueuedTaskRecord
    from litestar_queues.service import QueueService

__all__ = ("LocalExecutionBackend",)


class LocalExecutionBackend(BaseExecutionBackend):
    """Execution backend for in-process workers."""

    __slots__ = ()

    async def execute(self, service: "QueueService", record: "QueuedTaskRecord") -> "QueuedTaskRecord":
        """Execute one claimed task in a local worker.

        Returns:
            The updated queue record.
        """
        return await service.execute_record(record)

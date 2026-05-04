from typing import TYPE_CHECKING

from typing_extensions import Self

if TYPE_CHECKING:
    from litestar_queues.config import QueueConfig
    from litestar_queues.models import QueuedTaskRecord
    from litestar_queues.service import QueueService

__all__ = ("BaseExecutionBackend",)


class BaseExecutionBackend:
    """Base class for queue execution backends."""

    __slots__ = ("config",)

    def __init__(self, config: "QueueConfig | None" = None) -> None:
        """Initialize the execution backend."""
        self.config = config

    async def open(self) -> bool:
        """Open execution resources.

        Returns:
            True when resources are ready.
        """
        return True

    async def close(self) -> None:
        """Close execution resources."""

    async def execute(self, service: "QueueService", record: "QueuedTaskRecord") -> "QueuedTaskRecord":
        """Execute a queue record."""
        raise NotImplementedError

    async def dispatch(self, service: "QueueService", record: "QueuedTaskRecord") -> str | None:
        """Dispatch a queue record to an external executor.

        Returns:
            The external execution reference, if one was created.
        """
        await self.execute(service, record)
        return record.execution_ref

    async def reconcile(self, service: "QueueService", record: "QueuedTaskRecord") -> "QueuedTaskRecord | None":
        """Reconcile an externally running queue record.

        Returns:
            The updated record when reconciliation changes state.
        """
        return None

    async def cancel(self, service: "QueueService", record: "QueuedTaskRecord") -> bool:
        """Cancel an externally running queue record if possible.

        Returns:
            True when cancellation succeeds.
        """
        return False

    async def __aenter__(self) -> Self:
        await self.open()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        await self.close()

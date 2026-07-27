from dataclasses import dataclass
from typing import TYPE_CHECKING

from typing_extensions import Self

if TYPE_CHECKING:
    from datetime import timedelta
    from types import TracebackType

    from litestar_queues.config import QueueConfig
    from litestar_queues.models import QueuedTaskRecord
    from litestar_queues.service import QueueService

__all__ = ("BaseExecutionBackend", "DispatchRepairResult")


@dataclass(frozen=True, slots=True)
class DispatchRepairResult:
    """Outcome of one bounded delivery-repair pass.

    ``examined`` is how much of the caller's budget the pass consumed, whether
    or not a candidate needed anything done, so the caller can spend what is
    left on its other work.
    """

    examined: "int" = 0
    changed: "int" = 0


class BaseExecutionBackend:
    """Base class for queue execution backends."""

    __slots__ = ("config",)

    def __init__(self, config: "QueueConfig | None" = None) -> "None":
        """Initialize the execution backend."""
        self.config = config

    @property
    def is_external(self) -> "bool":
        """Whether this backend dispatches records to another process."""
        return False

    @property
    def schedules_on_enqueue(self) -> "bool":
        """Whether the backend schedules a persisted record without a Worker.

        Deliberately separate from :attr:`is_external`. Cloud Run Jobs and the
        broker backends are external but still rely on a worker loop noticing a
        pending record; only a managed transport that accepts the record itself
        answers true here.
        """
        return False

    @property
    def max_schedule_horizon(self) -> "timedelta | None":
        """How far ahead this backend will hold a scheduled delivery.

        ``None`` means unbounded, which is every backend that keeps due records
        in the queue store. A managed transport that takes ownership of the
        record has its own ceiling, and a schedule past it is not a call that
        fails once -- it is a recurrence that can never run.
        """
        return None

    async def schedule(self, service: "QueueService", record: "QueuedTaskRecord") -> "str | None":
        """Schedule one already-persisted record for external delivery.

        Returns:
            The external delivery reference, if one was created.
        """
        return await self.dispatch(service, record)

    async def repair(self, service: "QueueService", *, limit: "int") -> "DispatchRepairResult":
        """Recreate deliveries this backend owns that its transport no longer holds.

        A no-op for every backend whose records are found by polling: nothing
        can go missing from a store the worker reads directly. Only a managed
        transport that took ownership of the record can lose it silently.

        Args:
            service: The queue service whose records to repair.
            limit: Ceiling on how many records one pass may examine. Bounded
                maintenance is the only caller, and it always passes a positive
                budget it needs back.

        Returns:
            An empty result.
        """
        return DispatchRepairResult()

    async def open(self) -> "bool":
        """Open execution resources.

        Returns:
            True when resources are ready.
        """
        return True

    async def close(self) -> "None":
        """Close execution resources."""

    async def execute(
        self, service: "QueueService", record: "QueuedTaskRecord", *, worker_id: "str | None" = None
    ) -> "QueuedTaskRecord":
        """Execute a queue record."""
        raise NotImplementedError

    async def dispatch(self, service: "QueueService", record: "QueuedTaskRecord") -> "str | None":
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

    async def cancel(self, service: "QueueService", record: "QueuedTaskRecord") -> "bool":
        """Cancel an externally running queue record if possible.

        Returns:
            True when cancellation succeeds.
        """
        return False

    async def __aenter__(self) -> "Self":
        await self.open()
        return self

    async def __aexit__(
        self,
        exc_type: "type[BaseException] | None",  # noqa: PYI036
        exc_val: "BaseException | None",  # noqa: PYI036
        exc_tb: "TracebackType | None",  # noqa: PYI036
    ) -> "None":
        await self.close()

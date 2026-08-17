import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from typing_extensions import Self

from litestar_queues.namespace import QueueNamespace

if TYPE_CHECKING:
    from datetime import timedelta
    from types import TracebackType

    from litestar_queues.config import QueueConfig
    from litestar_queues.models import QueuedTaskRecord
    from litestar_queues.service import QueueService

__all__ = (
    "BaseConsumerExecutionBackend",
    "BaseExecutionBackend",
    "DispatchRepairResult",
    "ExecutionCancelResult",
    "ExecutionCancelStatus",
)

_MESSAGING_SYSTEM = "litestar_queues"


def _queue_observability_attributes(operation: "str", record: "QueuedTaskRecord") -> "dict[str, object]":
    """Build the span attributes for one execution-backend operation.

    Returns:
        Span attributes describing the record and the operation.
    """
    attributes: "dict[str, object]" = {
        "messaging.system": _MESSAGING_SYSTEM,
        "messaging.operation.name": operation,
        "messaging.destination.name": record.queue,
        "queue.task.name": record.task_name,
        "queue.execution.backend": record.execution_backend,
    }
    if record.execution_profile:
        attributes["queue.execution.profile"] = record.execution_profile
    return attributes


def _queue_metric_attributes(record: "QueuedTaskRecord") -> "dict[str, str]":
    """Build the label set every execution-backend metric family carries.

    Shared rather than per-backend because a Prometheus collector is registered
    once per metric name and registry, label names included: a backend that adds
    or drops one key raises on the first recording instead of opening a second
    series. Every value here is bounded by the deployment's own configuration --
    queue names, task names, backend and profile selectors -- and the operation's
    outcome is the only key a caller adds.

    Returns:
        The shared metric labels for this record.
    """
    return {
        "messaging.destination.name": record.queue,
        "queue.task.name": record.task_name,
        "queue.execution.backend": record.execution_backend,
        "queue.execution.profile": record.execution_profile or "",
    }


@dataclass(frozen=True, slots=True)
class DispatchRepairResult:
    """Outcome of one bounded delivery-repair pass.

    ``examined`` is how much of the caller's budget the pass consumed, whether
    or not a candidate needed anything done, so the caller can spend what is
    left on its other work.
    """

    examined: "int" = 0
    changed: "int" = 0


ExecutionCancelStatus = Literal["accepted", "already_cancelled", "retryable", "unsupported"]


@dataclass(frozen=True, slots=True)
class ExecutionCancelResult:
    """Outcome of one provider-level cancellation attempt.

    ``detail`` carries the provider's own words for the log line and the
    lifecycle event; it is never parsed.
    """

    status: "ExecutionCancelStatus"
    detail: "str | None" = None

    @property
    def permits_durable_cancel(self) -> "bool":
        """Whether this outcome may win the durable transition to cancelled.

        Only a control plane that exists and refused blocks the write. A
        transport with no control plane cannot strand anything, because the
        dispatch-by-id claim fence makes an in-flight delivery a no-op.

        Returns:
            True unless the provider refused a cancellation it could have made.
        """
        return self.status != "retryable"

    @classmethod
    def accepted(cls, detail: "str | None" = None) -> "Self":
        """Return an accepted cancellation result.

        Returns:
            A result whose status is ``accepted``.
        """
        return cls(status="accepted", detail=detail)

    @classmethod
    def already_cancelled(cls, detail: "str | None" = None) -> "Self":
        """Return an idempotent already-cancelled result.

        Returns:
            A result whose status is ``already_cancelled``.
        """
        return cls(status="already_cancelled", detail=detail)

    @classmethod
    def retryable(cls, detail: "str | None" = None) -> "Self":
        """Return a transient or rejected cancellation result.

        Returns:
            A result whose status is ``retryable``.
        """
        return cls(status="retryable", detail=detail)

    @classmethod
    def unsupported(cls, detail: "str | None" = None) -> "Self":
        """Return a result for a transport with no cancellation control plane.

        Returns:
            A result whose status is ``unsupported``.
        """
        return cls(status="unsupported", detail=detail)


class BaseExecutionBackend:
    """Base class for queue execution backends."""

    __slots__ = ("_logger", "config")

    def __init__(self, config: "QueueConfig | None" = None) -> "None":
        """Initialize the execution backend."""
        self.config = config
        names = config.names if config is not None else QueueNamespace()
        self._logger = logging.getLogger(names.logger("execution", type(self).__name__))

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

    async def cancel_execution(self, service: "QueueService", record: "QueuedTaskRecord") -> "ExecutionCancelResult":
        """Cancel the provider resource backing one external attempt.

        Called before any durable transition to ``cancelled`` for a record whose
        ``execution_ref`` names a live provider resource. Implementations must
        contain their client's exceptions and answer with a result: a not-found
        resource is ``already_cancelled``, anything transient or refused is
        ``retryable``. Raising is reserved for programming errors.

        Returns:
            ``unsupported`` for a transport with no cancellation control plane.
        """
        del service, record
        return ExecutionCancelResult.unsupported()

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


class BaseConsumerExecutionBackend(BaseExecutionBackend):
    """Base for external backends that continuously receive broker deliveries."""

    __slots__ = ()

    async def run_consumer(self, service: "QueueService", *, max_concurrency: "int", drain_timeout: "float") -> "None":
        """Receive and execute deliveries until cancelled."""
        raise NotImplementedError

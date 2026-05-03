from typing import TYPE_CHECKING, Any

from typing_extensions import Self

if TYPE_CHECKING:
    from datetime import datetime, timedelta
    from uuid import UUID

    from litestar_queues.config import QueueConfig
    from litestar_queues.models import QueuedTaskRecord

__all__ = ("BaseStorageBackend",)


class BaseStorageBackend:
    """Base class for queue storage backends."""

    __slots__ = ("config",)

    def __init__(self, config: "QueueConfig | None" = None) -> None:
        """Initialize the storage backend."""
        self.config = config

    async def open(self) -> bool:
        """Open storage resources.

        Returns:
            True when resources are ready.
        """
        return True

    async def close(self) -> None:
        """Close storage resources."""

    async def enqueue(
        self,
        task_name: str,
        *,
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
        queue: str = "default",
        priority: int = 0,
        max_retries: int = 0,
        scheduled_at: "datetime | None" = None,
        key: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> "QueuedTaskRecord":
        """Persist a queued task."""
        raise NotImplementedError

    async def get_task(self, task_id: "UUID") -> "QueuedTaskRecord | None":
        """Return a queued task by ID."""
        raise NotImplementedError

    async def get_task_by_key(self, key: str) -> "QueuedTaskRecord | None":
        """Return a queued task by deduplication key."""
        raise NotImplementedError

    async def list_pending(
        self,
        *,
        limit: int = 1,
        queue: str | None = None,
    ) -> "list[QueuedTaskRecord]":
        """Return due pending or scheduled tasks ordered for execution."""
        raise NotImplementedError

    async def claim_task(self, task_id: "UUID") -> "QueuedTaskRecord | None":
        """Atomically claim a pending task."""
        raise NotImplementedError

    async def claim_next(self, *, queue: str | None = None) -> "QueuedTaskRecord | None":
        """Claim the next due task.

        Returns:
            The claimed task record, if one was available.
        """
        records = await self.list_pending(limit=1, queue=queue)
        if not records:
            return None
        return await self.claim_task(records[0].id)

    async def complete_task(self, task_id: "UUID", *, result: Any = None) -> "QueuedTaskRecord | None":
        """Mark a task as completed."""
        raise NotImplementedError

    async def fail_task(
        self,
        task_id: "UUID",
        error: str,
        *,
        retry: bool = True,
    ) -> "QueuedTaskRecord | None":
        """Mark a task as failed or retry it."""
        raise NotImplementedError

    async def cancel_task(self, task_id: "UUID") -> bool:
        """Cancel a task if it has not started."""
        raise NotImplementedError

    async def touch_heartbeat(self, task_id: "UUID") -> None:
        """Update the heartbeat timestamp for a running task."""

    async def requeue_stale_running(self, *, stale_after: "timedelta") -> int:
        """Requeue running tasks with stale heartbeats.

        Returns:
            Number of requeued tasks.
        """
        return 0

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

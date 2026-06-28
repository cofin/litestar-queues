from typing import TYPE_CHECKING, Any

from litestar.background_tasks import BackgroundTask

from litestar_queues.task import get_default_service

if TYPE_CHECKING:
    from litestar_queues.service import QueueService
    from litestar_queues.task import Task

__all__ = ("QueuedBackgroundTask",)


class QueuedBackgroundTask(BackgroundTask):
    """Background task that enqueues a queue task after the response is sent."""

    def __init__(
        self, task: "str | Task[Any, Any]", *args: "Any", service: "QueueService | None" = None, **kwargs: "Any"
    ) -> "None":
        """Initialize a queued background task.

        Args:
            task: The registered task name or wrapper.
            *args: Positional arguments to pass to the task.
            service: Optional custom queue service. If not provided, the default service is resolved.
            **kwargs: Keyword arguments for enqueue and the task.

        Raises:
            RuntimeError: If no active QueueService can be resolved.
        """
        resolved_service = service or get_default_service()
        if resolved_service is None:
            msg = "No active QueueService is registered. Is the QueuePlugin registered on your app?"
            raise RuntimeError(msg)
        super().__init__(resolved_service.enqueue, task, *args, **kwargs)

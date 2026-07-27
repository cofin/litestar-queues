"""Cloud Tasks execution backend.

Constructing the backend never builds a Google client: the client is resolved on
first use so an installation without the ``cloud-tasks`` extra can still import,
configure, and validate this backend.
"""

from importlib import import_module
from typing import TYPE_CHECKING, cast

from litestar_queues.exceptions import MissingDependencyError
from litestar_queues.execution.base import BaseExecutionBackend
from litestar_queues.execution.cloudtasks.config import CloudTasksExecutionConfig, _execution_config_from_queue_config

if TYPE_CHECKING:
    from litestar_queues.config import QueueConfig
    from litestar_queues.execution.cloudtasks._typing import CloudTasksClient

__all__ = ("CloudTasksExecutionBackend",)

_GOOGLE_CLOUD_TASKS_PACKAGE = "google-cloud-tasks"
_CLOUD_TASKS_EXTRA = "cloud-tasks"


class CloudTasksExecutionBackend(BaseExecutionBackend):
    """Execution backend that hands persisted records to Google Cloud Tasks."""

    __slots__ = ("_execution_config", "client")

    def __init__(
        self,
        config: "QueueConfig | None" = None,
        *,
        execution_config: "CloudTasksExecutionConfig | None" = None,
        client: "CloudTasksClient | None" = None,
    ) -> "None":
        """Initialize the Cloud Tasks execution backend."""
        super().__init__(config=config)
        self._execution_config = execution_config
        self.client = client

    @property
    def is_external(self) -> "bool":
        """Whether this backend dispatches records to another process."""
        return True

    @property
    def execution_config(self) -> "CloudTasksExecutionConfig":
        """Resolved Cloud Tasks execution config."""
        if self._execution_config is not None:
            return self._execution_config
        return _execution_config_from_queue_config(self.config)

    async def _get_client(self) -> "CloudTasksClient":
        """Return the Cloud Tasks client, creating it on first use.

        Returns:
            The Cloud Tasks async client.

        Raises:
            MissingDependencyError: If the ``cloud-tasks`` extra is not installed.
        """
        if self.client is None:
            try:
                tasks_v2 = import_module("google.cloud.tasks_v2")
            except ImportError as exc:
                raise MissingDependencyError(_GOOGLE_CLOUD_TASKS_PACKAGE, _CLOUD_TASKS_EXTRA) from exc
            self.client = cast("CloudTasksClient", tasks_v2.CloudTasksAsyncClient())
        return self.client

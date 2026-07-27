"""Public structural types for the Cloud Tasks execution backend.

The supported import location for
:mod:`litestar_queues.execution.cloudtasks._typing`. These protocols describe
the Google client without importing it, so a test or adapter can substitute one
without the ``cloud-tasks`` extra installed.
"""

from litestar_queues.execution.cloudtasks._typing import CloudTasksClient, CloudTasksTaskLike

__all__ = ("CloudTasksClient", "CloudTasksTaskLike")

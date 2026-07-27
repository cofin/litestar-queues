"""Public structural types for the Cloud Run Jobs execution backend.

The supported import location for
:mod:`litestar_queues.execution.cloudrun._typing`. These protocols describe the
Google clients without importing them, so a test or adapter can substitute one
without the ``cloudrun`` extra installed.
"""

from litestar_queues.execution.cloudrun._typing import (
    CloudRunExecutionLike,
    CloudRunExecutionsClient,
    CloudRunJobsClient,
    CloudRunOperation,
)

__all__ = ("CloudRunExecutionLike", "CloudRunExecutionsClient", "CloudRunJobsClient", "CloudRunOperation")

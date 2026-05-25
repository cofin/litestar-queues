from typing import Any, Protocol

__all__ = ("CloudRunExecutionLike", "CloudRunExecutionsClient", "CloudRunJobsClient", "CloudRunOperation")


class CloudRunExecutionLike(Protocol):
    """Protocol for the subset of Cloud Run Execution fields used here."""

    name: str
    succeeded_count: int
    failed_count: int
    cancelled_count: int
    conditions: list[Any] | None


class CloudRunOperation(Protocol):
    """Protocol for Cloud Run long-running operations."""

    async def result(self) -> CloudRunExecutionLike:
        """Return the created Cloud Run execution."""
        ...


class CloudRunJobsClient(Protocol):
    """Protocol for the Cloud Run Jobs async client."""

    async def run_job(self, *, request: dict[str, Any]) -> CloudRunOperation:
        """Run a Cloud Run job."""
        ...


class CloudRunExecutionsClient(Protocol):
    """Protocol for the Cloud Run Executions async client."""

    async def get_execution(self, *, name: str) -> CloudRunExecutionLike:
        """Return a Cloud Run execution."""
        ...

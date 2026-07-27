"""Structural types for the Google Cloud Tasks client.

Protocols keep ``google-cloud-tasks`` out of the import graph and let tests
substitute a client without the extra installed.
"""

from typing import Any, Protocol

__all__ = ("CloudTasksClient", "CloudTasksTaskLike")


class CloudTasksTaskLike(Protocol):
    """Protocol for the subset of a created Cloud Tasks task used here."""

    name: "str"


class CloudTasksClient(Protocol):
    """Protocol for the Cloud Tasks async client."""

    async def create_task(self, *, request: "dict[str, Any]") -> "CloudTasksTaskLike":
        """Create one task on a Cloud Tasks queue."""
        ...

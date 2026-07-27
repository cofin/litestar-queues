"""Injected stand-ins for the Google Cloud Tasks async client.

Nothing here imports ``google-cloud-tasks``. That is the point: the request the
package builds has to be ordinary Python data, so these fakes can assert on it
byte for byte without the optional extra, and the API error classes are matched
structurally rather than by identity.
"""

from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

__all__ = ("AlreadyExists", "CreateCall", "FakeCloudTasksClient", "NotFound", "ServiceUnavailable")

_CONFLICT = 409
_NOT_FOUND = 404
_UNAVAILABLE = 503


class GoogleApiError(Exception):
    """Shape-compatible stand-in for a ``google.api_core`` error.

    Real ``google.api_core`` errors expose ``code`` as an ``HTTPStatus``, which
    compares equal to the plain integers used here.
    """

    code: "int" = 0


class AlreadyExists(GoogleApiError):  # noqa: N818 - matches the google.api_core class name the backend detects.
    """The named task already exists on the queue."""

    code = _CONFLICT


class NotFound(GoogleApiError):  # noqa: N818 - matches the google.api_core class name the backend detects.
    """The named task is absent from the queue."""

    code = _NOT_FOUND


class ServiceUnavailable(GoogleApiError):  # noqa: N818 - matches google.api_core naming.
    """The API call did not reach a definite answer."""

    code = _UNAVAILABLE


@dataclass(slots=True)
class CreateCall:
    """One recorded ``create_task`` invocation."""

    request: "dict[str, Any]"
    timeout: "float | None"

    @property
    def parent(self) -> "str":
        """Queue path the task was created under."""
        return str(self.request["parent"])

    @property
    def task(self) -> "dict[str, Any]":
        """The task message the package built."""
        return dict(self.request["task"])

    @property
    def name(self) -> "str":
        """Full resource name of the created task."""
        return str(self.task["name"])

    @property
    def http_request(self) -> "dict[str, Any]":
        """The HTTP delivery description."""
        return dict(self.task["http_request"])

    @property
    def body(self) -> "bytes":
        """Raw bytes that cross the transport."""
        return bytes(self.http_request["body"])


@dataclass(slots=True)
class FakeCloudTasksClient:
    """Records every call and replays whatever outcome a test asks for.

    ``on_create`` runs after the call is recorded and before a result is
    returned, so a test can raise from it, or mutate the queue store mid-call to
    reproduce a concurrent writer.
    """

    on_create: "Callable[[CreateCall], Awaitable[None]] | None" = None
    on_get: "Callable[[str], Awaitable[None]] | None" = None
    existing: "set[str]" = field(default_factory=set)
    create_calls: "list[CreateCall]" = field(default_factory=list)
    get_calls: "list[str]" = field(default_factory=list)
    delete_calls: "list[str]" = field(default_factory=list)
    close_calls: "int" = 0

    async def create_task(self, *, request: "dict[str, Any]", timeout: "float | None" = None) -> "Any":
        """Record a creation and return the created task.

        Returns:
            An object carrying the created task's full resource name.
        """
        call = CreateCall(request=request, timeout=timeout)
        self.create_calls.append(call)
        if self.on_create is not None:
            await self.on_create(call)
        self.existing.add(call.name)
        return SimpleNamespace(name=call.name)

    async def get_task(self, *, name: "str", timeout: "float | None" = None) -> "Any":
        """Look up one task by full resource name.

        Returns:
            An object carrying the task's full resource name.

        Raises:
            NotFound: If the queue holds no task under ``name``.
        """
        del timeout
        self.get_calls.append(name)
        if self.on_get is not None:
            await self.on_get(name)
        if name not in self.existing:
            msg = "task not found"
            raise NotFound(msg)
        return SimpleNamespace(name=name)

    async def delete_task(self, *, name: "str", timeout: "float | None" = None) -> "None":
        """Delete one task by full resource name.

        Raises:
            NotFound: If the queue holds no task under ``name``.
        """
        del timeout
        self.delete_calls.append(name)
        if name not in self.existing:
            msg = "task not found"
            raise NotFound(msg)
        self.existing.discard(name)

    async def close(self) -> "None":
        """Release the client's transport."""
        self.close_calls += 1

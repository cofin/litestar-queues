"""Private structural types for Google Pub/Sub clients."""

from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


class PubSubPublisherClient(Protocol):
    """Publisher operations used by the execution backend."""

    async def publish(self, *, request: "dict[str, Any]", timeout: "float") -> "object": ...

    async def close(self) -> "None": ...


class PubSubSubscriberClient(Protocol):
    """Subscriber operations used by the execution backend."""

    def streaming_pull(self, *, requests: "AsyncIterator[Any]") -> "Any": ...

    async def close(self) -> "None": ...

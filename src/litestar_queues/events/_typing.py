"""Internal event backend typing helpers."""

from typing import TYPE_CHECKING, Protocol, TypeAlias

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Sequence
    from contextlib import AbstractAsyncContextManager


class ChannelsSubscriber(Protocol):
    """Subscription object returned by ChannelsPlugin-style backends."""

    def iter_events(self) -> "AsyncIterator[bytes]": ...


class ChannelsPublishBackend(Protocol):
    """Backend that publishes channel payloads directly."""

    def publish(self, data: bytes | str, channels: "Sequence[str]") -> object: ...


class ChannelsWaitPublishedBackend(Protocol):
    """Channels plugin variant that waits until publication completes."""

    def wait_published(self, data: bytes | str, channels: "Sequence[str]") -> object: ...


class ChannelsWaitPublishedManyBackend(Protocol):
    """Channels plugin variant that waits until batch publication completes."""

    def wait_published_many(self, data: "Sequence[bytes | str]", channels: "Sequence[str]") -> object: ...


class ChannelsSubscriptionBackend(Protocol):
    """Channels plugin variant that exposes a subscription context manager."""

    def start_subscription(
        self, channels: "Sequence[str]", history: int = 0
    ) -> "AbstractAsyncContextManager[ChannelsSubscriber]": ...


class ChannelsStreamBackend(Protocol):
    """Channels backend variant with explicit subscribe and stream calls."""

    async def subscribe(self, channels: "Sequence[str]") -> object: ...

    async def unsubscribe(self, channels: "Sequence[str]") -> object: ...

    def stream_events(self) -> "AsyncIterator[tuple[str, bytes]]": ...


ChannelsLike: TypeAlias = (
    ChannelsPublishBackend
    | ChannelsWaitPublishedBackend
    | ChannelsWaitPublishedManyBackend
    | ChannelsSubscriptionBackend
    | ChannelsStreamBackend
)

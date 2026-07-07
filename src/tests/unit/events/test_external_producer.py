from typing import TYPE_CHECKING

import pytest

from litestar_queues import EventConfig, QueueConfig
from litestar_queues.events import QueueChannels, QueueEvent

if TYPE_CHECKING:
    from collections.abc import Sequence

pytestmark = pytest.mark.anyio


async def test_factory_opens_and_closes_channels_backend() -> None:
    from litestar_queues.events import create_event_producer

    backend = _RecordingChannelsBackend()

    async with create_event_producer(QueueConfig(event=EventConfig(channels_backend=backend))) as producer:
        await producer.channel("imports:acme").publish("import.retry_requested")
        assert backend.open_count == 1
        assert backend.close_count == 0

    assert backend.close_count == 1
    [(payload, channels)] = backend.published
    event = QueueEvent.from_json(payload)
    assert event.type == "import.retry_requested"
    assert channels == (QueueChannels.custom("imports:acme"),)


async def test_factory_opens_no_queue_backend_or_worker() -> None:
    from litestar_queues.events import create_event_producer

    config = _ExplodingBackendConfig(event=EventConfig(channels_backend=_RecordingChannelsBackend()))

    async with create_event_producer(config) as producer:
        await producer.channel("imports:acme").publish("x")


async def test_factory_tolerates_backend_without_open_close() -> None:
    from litestar_queues.events import create_event_producer

    backend = _PublishOnlyChannelsBackend()

    async with create_event_producer(QueueConfig(event=EventConfig(channels_backend=backend))) as producer:
        await producer.channel("imports:acme").publish("x")

    assert len(backend.published) == 1


async def test_factory_strict_propagates() -> None:
    from litestar_queues.events import create_event_producer

    async with create_event_producer(
        QueueConfig(event=EventConfig(strict=True, channels_backend=_FailingChannelsBackend()))
    ) as producer:
        with pytest.raises(RuntimeError, match="publish failed"):
            await producer.channel("imports:acme").publish("x")


async def test_manual_aclose() -> None:
    from litestar_queues.events import create_event_producer

    backend = _RecordingChannelsBackend()
    external = create_event_producer(QueueConfig(event=EventConfig(channels_backend=backend)))
    producer = await external.__aenter__()
    await producer.channel("imports:acme").publish("x")
    await external.aclose()

    assert backend.open_count == 1
    assert backend.close_count == 1


async def test_manual_close_alias() -> None:
    from litestar_queues.events import create_event_producer

    backend = _RecordingChannelsBackend()
    external = create_event_producer(QueueConfig(event=EventConfig(channels_backend=backend)))
    await external.__aenter__()
    await external.close()

    assert backend.open_count == 1
    assert backend.close_count == 1


class _RecordingChannelsBackend:
    def __init__(self) -> None:
        self.open_count = 0
        self.close_count = 0
        self.published: "list[tuple[bytes | str, tuple[str, ...]]]" = []

    async def open(self) -> None:
        self.open_count += 1

    async def close(self) -> None:
        self.close_count += 1

    async def publish(self, data: "bytes | str", channels: "Sequence[str]") -> None:
        self.published.append((data, tuple(channels)))


class _PublishOnlyChannelsBackend:
    def __init__(self) -> None:
        self.published: "list[tuple[bytes | str, tuple[str, ...]]]" = []

    async def publish(self, data: "bytes | str", channels: "Sequence[str]") -> None:
        self.published.append((data, tuple(channels)))


class _FailingChannelsBackend:
    async def publish(self, data: "bytes | str", channels: "Sequence[str]") -> None:
        msg = "publish failed"
        raise RuntimeError(msg)


class _ExplodingBackendConfig(QueueConfig):
    def get_queue_backend(self) -> object:
        msg = "queue backend must not open"
        raise AssertionError(msg)

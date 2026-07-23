import asyncio
from typing import TYPE_CHECKING

import pytest

from litestar_queues import EventDeliveryConfig, QueueConfig, QueueService
from litestar_queues.events import EventBufferConfig, QueueEvent, QueueEventsConfig

if TYPE_CHECKING:
    from collections.abc import Sequence

pytestmark = pytest.mark.anyio


async def test_service_close_drains_buffer_before_sink_close() -> None:
    sink = _OrderingSink()
    service = QueueService(
        QueueConfig(
            events=QueueEventsConfig(
                delivery=EventDeliveryConfig(sinks=(sink,), buffer=EventBufferConfig(batch_size=10, flush_interval=60))
            )
        )
    )

    await service.open()
    await service.get_event_publisher().publish(QueueEvent(type="task.progress", scope="task", task_id="task-a"))

    assert sink.operations == ["open"]

    await service.close()

    assert sink.operations == ["open", "publish:task.progress", "close"]


async def test_service_open_starts_flush_loop() -> None:
    sink = _OrderingSink()
    service = QueueService(
        QueueConfig(
            events=QueueEventsConfig(
                delivery=EventDeliveryConfig(
                    sinks=(sink,), buffer=EventBufferConfig(batch_size=10, flush_interval=0.01)
                )
            )
        )
    )

    await service.open()
    try:
        await service.get_event_publisher().publish(QueueEvent(type="task.progress", scope="task", task_id="task-a"))

        assert sink.published == []

        await asyncio.wait_for(sink.published_event.wait(), timeout=1)
    finally:
        await service.close()

    assert [event.type for event in sink.published] == ["task.progress"]


class _OrderingSink:
    def __init__(self) -> None:
        self.operations: "list[str]" = []
        self.published: "list[QueueEvent]" = []
        self.published_event = asyncio.Event()

    async def open(self) -> None:
        self.operations.append("open")

    async def close(self) -> None:
        self.operations.append("close")

    async def publish(self, event: "QueueEvent", *, channels: "Sequence[str]") -> None:
        self.operations.append(f"publish:{event.type}")
        self.published.append(event)
        self.published_event.set()

import asyncio
from typing import TYPE_CHECKING

import pytest

from litestar_queues import EventDeliveryConfig, QueueConfig, QueueService, WorkerConfig
from litestar_queues.events import EventBufferConfig, QueueEvent, QueueEventsConfig

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Sequence

    from litestar_queues.events import QueueEventLogRecord

pytestmark = pytest.mark.anyio


async def test_service_close_drains_buffer_before_sink_close() -> None:
    sink = _OrderingSink()
    service = QueueService(
        QueueConfig(
            worker=WorkerConfig(placement="external"),
            queue_backend="memory",
            events=QueueEventsConfig(
                delivery=EventDeliveryConfig(sinks=(sink,), buffer=EventBufferConfig(batch_size=10, flush_interval=60))
            ),
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
            worker=WorkerConfig(placement="external"),
            queue_backend="memory",
            events=QueueEventsConfig(
                delivery=EventDeliveryConfig(
                    sinks=(sink,), buffer=EventBufferConfig(batch_size=10, flush_interval=0.01)
                )
            ),
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


@pytest.mark.parametrize("live_buffer", [False, True])
@pytest.mark.parametrize("during_close", [False, True])
@pytest.mark.parametrize("action", ["open", "close"])
async def test_service_rejects_lifecycle_from_history_release(
    monkeypatch: "pytest.MonkeyPatch", *, live_buffer: bool, during_close: bool, action: str
) -> None:
    from litestar_queues.backends.memory.backend import InMemoryQueueBackend
    from litestar_queues.backends.memory.event_log import InMemoryQueueEventLog
    from litestar_queues.events import EventHistoryConfig
    from litestar_queues.events._history_buffer import _HistoryBuffer
    from litestar_queues.events._log_records import event_log_record_from_event
    from litestar_queues.exceptions import QueueConfigurationError

    config = EventHistoryConfig(batch_size=20, flush_interval=60, strict=True)

    class BufferedHistory(InMemoryQueueEventLog):
        def __init__(self) -> None:
            super().__init__(config)
            self.buffer = _HistoryBuffer(config, self.write)

        async def write(self, records: "Sequence[QueueEventLogRecord]") -> None:
            self._records.extend(records)

        async def publish_event_after_commit(
            self, event: QueueEvent, *, release: "Callable[[], Awaitable[None]]", barrier: bool = False
        ) -> None:
            await self.buffer.enqueue(event_log_record_from_event(event), release=release, barrier=barrier)

        async def flush_events(self) -> None:
            await self.buffer.flush()

        async def aclose(self) -> None:
            await self.buffer.stop()

    history = BufferedHistory()
    monkeypatch.setattr(InMemoryQueueBackend, "get_event_log", lambda _self, _config: history)
    outcomes: list[BaseException | None] = []

    class CallbackSink(_OrderingSink):
        async def publish(self, event: QueueEvent, *, channels: "Sequence[str]") -> None:
            await super().publish(event, channels=channels)
            if len(self.published) != 1:
                return

            async def attempt() -> None:
                if action == "open":
                    await service.open()
                else:
                    await service.close()

            try:
                await asyncio.wait_for(attempt(), timeout=0.2)
            except (QueueConfigurationError, asyncio.TimeoutError, asyncio.CancelledError) as exc:
                outcomes.append(exc)
            else:
                outcomes.append(None)

    sink = CallbackSink()
    service = QueueService(
        QueueConfig(
            worker=WorkerConfig(placement="external"),
            queue_backend="memory",
            events=QueueEventsConfig(
                history=config,
                delivery=EventDeliveryConfig(
                    sinks=(sink,), buffer=EventBufferConfig(batch_size=20, flush_interval=60) if live_buffer else None
                ),
            ),
        )
    )
    await service.open()
    try:
        await service.get_event_publisher().publish(QueueEvent(type="task.progress", scope="task", task_id="first"))
        if during_close:
            await service.close()
        else:
            await history.flush_events()
            await service.get_event_publisher().flush_buffer()
        assert len(outcomes) == 1
        assert isinstance(outcomes[0], QueueConfigurationError), outcomes
        if not during_close:
            assert service.get_event_log() is history
            assert sink.operations.count("close") == 0
            await service.get_event_publisher().publish(
                QueueEvent(type="task.completed", scope="task", task_id="second")
            )
            assert [event.task_id for event in sink.published] == ["first", "second"]
    finally:
        await service.close()

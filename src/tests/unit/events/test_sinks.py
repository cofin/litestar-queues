from typing import TYPE_CHECKING

import pytest

from litestar_queues.events import InMemoryQueueEventSink, NoopQueueEventSink, QueueChannels, QueueEvent

if TYPE_CHECKING:
    from collections.abc import Sequence

pytestmark = pytest.mark.anyio


async def test_inmemory_publish_many_records_all_in_order() -> None:
    sink = InMemoryQueueEventSink()
    first = QueueEvent(type="task.progress", scope="task", task_id="task-a")
    second = QueueEvent(type="task.log", scope="task", task_id="task-a")

    await sink.publish_many(
        (
            (first, (QueueChannels.task("task-a"),)),
            (second, (QueueChannels.task("task-a"), QueueChannels.global_channel())),
        )
    )

    assert sink.events == [first, second]
    assert sink.events_for(QueueChannels.task("task-a")) == [first, second]
    assert sink.events_for(QueueChannels.global_channel()) == [second]


async def test_noop_publish_many_drops() -> None:
    sink = NoopQueueEventSink()
    event = QueueEvent(type="task.progress", scope="task", task_id="task-a")

    assert await sink.publish_many(((event, ("tasks",)),)) is None


async def test_default_publish_many_loops_publish() -> None:
    from litestar_queues.events.sinks import default_publish_many

    sink = _PublishOnlySink()
    first = QueueEvent(type="task.progress", scope="task", task_id="task-a")
    second = QueueEvent(type="task.log", scope="task", task_id="task-a")

    await default_publish_many(sink, ((first, ("tasks",)), (second, ("tasks", "global"))))

    assert sink.published == [(first, ("tasks",)), (second, ("tasks", "global"))]


class _PublishOnlySink:
    def __init__(self) -> None:
        self.published: "list[tuple[QueueEvent, tuple[str, ...]]]" = []

    async def publish(self, event: "QueueEvent", *, channels: "Sequence[str]") -> None:
        self.published.append((event, tuple(channels)))

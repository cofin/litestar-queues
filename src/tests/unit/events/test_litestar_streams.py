import base64
from typing import TYPE_CHECKING, Any, cast

import msgspec
import pytest

from litestar_queues.events import QueueEvent
from litestar_queues.events.litestar import ChannelsQueueEventSink

if TYPE_CHECKING:
    from collections.abc import Sequence

pytestmark = pytest.mark.anyio


async def test_publish_many_groups_single_channel_into_one_call() -> "None":
    backend = _BatchPublishingChannelsBackend()
    sink = ChannelsQueueEventSink(cast("Any", backend))
    first = QueueEvent(type="task.progress", scope="task", task_id="task-1")
    second = QueueEvent(type="task.log", scope="task", task_id="task-1")

    await sink.publish_many(((first, ("events",)), (second, ("events",))))

    assert len(backend.published_many) == 1
    payloads, channels = backend.published_many[0]
    assert channels == ("events",)
    assert [QueueEvent.from_json(payload).type for payload in payloads] == ["task.progress", "task.log"]


async def test_publish_many_fallback_loops_when_no_batch_api() -> "None":
    backend = _WaitPublishingChannelsBackend()
    sink = ChannelsQueueEventSink(backend)
    first = QueueEvent(type="task.progress", scope="task", task_id="task-1")
    second = QueueEvent(type="task.log", scope="task", task_id="task-1")

    await sink.publish_many(((first, ("events",)), (second, ("events",))))

    assert len(backend.published) == 2
    assert [QueueEvent.from_json(payload).type for payload, _ in backend.published] == ["task.progress", "task.log"]
    assert {channels for _, channels in backend.published} == {("events",)}


async def test_publish_many_honours_max_payload_bytes() -> "None":
    items = [{"message": "a" * 100}, {"message": "b" * 100}, {"message": "c" * 100}]
    event = QueueEvent(
        type="task.event", scope="task", task_id="task-1", payload={"batch": True, "count": len(items), "items": items}
    )
    single_item_event = QueueEvent(
        type="task.event", scope="task", task_id="task-1", payload={"batch": True, "count": 1, "items": [items[0]]}
    )
    max_bytes = _estimate_wrapped_event_bytes(single_item_event) + 16
    backend = _BatchPublishingChannelsBackend()
    sink = ChannelsQueueEventSink(
        cast("Any", backend), max_payload_bytes=max_bytes, payload_size_estimator=_estimate_wrapped_event_bytes
    )

    await sink.publish_many(((event, ("events",)),))

    [(payloads, channels)] = backend.published_many
    assert channels == ("events",)
    assert len(payloads) > 1
    decoded = [QueueEvent.from_json(payload) for payload in payloads]
    assert [item for chunk in decoded for item in chunk.payload["items"]] == items
    assert all(_estimate_wrapped_event_bytes(decoded_event) <= max_bytes for decoded_event in decoded)


class _BatchPublishingChannelsBackend:
    def __init__(self) -> None:
        self.published_many: "list[tuple[tuple[bytes | str, ...], tuple[str, ...]]]" = []

    def wait_published_many(self, data: "Sequence[bytes | str]", channels: "Sequence[str]") -> None:
        self.published_many.append((tuple(data), tuple(channels)))


class _WaitPublishingChannelsBackend:
    def __init__(self) -> None:
        self.published: "list[tuple[bytes | str, tuple[str, ...]]]" = []

    def wait_published(self, data: "bytes | str", channels: "Sequence[str]") -> None:
        self.published.append((data, tuple(channels)))


def _estimate_wrapped_event_bytes(event: "QueueEvent") -> "int":
    payload = {
        "payload": {"data_b64": base64.b64encode(event.to_json()).decode("ascii")},
        "metadata": {"transport": "test"},
    }
    return len(msgspec.json.encode(payload))

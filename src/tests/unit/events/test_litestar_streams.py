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


@pytest.mark.parametrize("backend_kind", ["sqlspec", "redis"])
async def test_publish_many_keeps_channel_groups_separate(backend_kind: "str") -> "None":
    backend = _RedisPipelineChannelsBackend() if backend_kind == "redis" else _BatchPublishingChannelsBackend()
    sink = ChannelsQueueEventSink(cast("Any", backend))
    first = QueueEvent(type="task.progress", scope="task", task_id="task-1")
    second = QueueEvent(type="task.log", scope="task", task_id="task-2")
    third = QueueEvent(type="task.event", scope="queue", scope_key="critical", queue="critical")

    await sink.publish_many((
        (first, ("task-1", "global")),
        (second, ("task-2", "global")),
        (third, ("task-1", "global")),
    ))

    assert len(backend.published_many) == 2
    assert [channels for _, channels in backend.published_many] == [("task-1", "global"), ("task-2", "global")]
    assert [[QueueEvent.from_json(payload).type for payload in payloads] for payloads, _ in backend.published_many] == [
        ["task.progress", "task.event"],
        ["task.log"],
    ]


async def test_publish_many_uses_valkey_native_batch_capability_without_redis_import() -> "None":
    backend = _ValkeyPipelineChannelsBackend()
    sink = ChannelsQueueEventSink(cast("Any", backend))
    first = QueueEvent(type="task.progress", scope="task", task_id="task-1")
    second = QueueEvent(type="task.log", scope="task", task_id="task-1")

    await sink.publish_many(((first, ("events",)), (second, ("events",))))

    assert backend.transport_label == "valkey"
    assert backend.pipeline_count == 1
    [(payloads, channels)] = backend.published_many
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
    transport_label = "sqlspec"

    def __init__(self) -> None:
        self.published_many: "list[tuple[tuple[bytes | str, ...], tuple[str, ...]]]" = []

    async def publish_many(self, data: "Sequence[bytes | str]", channels: "Sequence[str]") -> None:
        self.published_many.append((tuple(data), tuple(channels)))


class _RedisPipelineChannelsBackend(_BatchPublishingChannelsBackend):
    transport_label = "redis"

    def __init__(self) -> None:
        super().__init__()
        self.pipeline_count = 0

    async def publish_many(self, data: "Sequence[bytes | str]", channels: "Sequence[str]") -> None:
        self.pipeline_count += 1
        await super().publish_many(data, channels)


class _ValkeyPipelineChannelsBackend(_RedisPipelineChannelsBackend):
    transport_label = "valkey"


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

import base64
from typing import TYPE_CHECKING

import msgspec
import pytest

from litestar_queues import EventDeliveryConfig, QueueConfig
from litestar_queues.events import QueueEvent, QueueEventsConfig, create_event_producer

if TYPE_CHECKING:
    from collections.abc import Sequence

pytestmark = pytest.mark.anyio


async def test_max_payload_bytes_flows_to_channels_sink() -> None:
    items = [{"message": "a" * 100}, {"message": "b" * 100}, {"message": "c" * 100}]
    event = QueueEvent(
        type="task.event", scope="task", task_id="task-1", payload={"batch": True, "count": len(items), "items": items}
    )
    single_item_event = QueueEvent(
        type="task.event", scope="task", task_id="task-1", payload={"batch": True, "count": 1, "items": [items[0]]}
    )
    backend = _RecordingChannelsBackend()
    config = QueueConfig(
        events=QueueEventsConfig(
            channels=backend,
            delivery=EventDeliveryConfig(
                buffer=None,
                max_payload_bytes=_estimate_wrapped_event_bytes(single_item_event) + 16,
                payload_size_estimator=_estimate_wrapped_event_bytes,
            ),
        )
    )

    await config.get_event_publisher().publish(event, channels=("events",))

    assert len(backend.published) > 1
    decoded = [QueueEvent.from_json(payload) for payload, _ in backend.published]
    assert [item for chunk in decoded for item in chunk.payload["items"]] == items


async def test_no_payload_limit_by_default() -> None:
    event = QueueEvent(
        type="task.event",
        scope="task",
        task_id="task-1",
        payload={"batch": True, "count": 2, "items": [{"message": "a"}, {"message": "b"}]},
    )
    backend = _RecordingChannelsBackend()
    config = QueueConfig(events=QueueEventsConfig(channels=backend, delivery=EventDeliveryConfig(buffer=None)))

    await config.get_event_publisher().publish(event, channels=("events",))

    assert len(backend.published) == 1
    [(payload, channels)] = backend.published
    assert QueueEvent.from_json(payload).payload["count"] == 2
    assert channels == ("litestar_queues:task:task-1:events", "events")


async def test_external_producer_uses_configured_payload_limit() -> None:
    items = [{"message": "a" * 100}, {"message": "b" * 100}, {"message": "c" * 100}]
    single_item_event = QueueEvent(
        type="task.event", scope="task", task_id="task-1", payload={"batch": True, "count": 1, "items": [items[0]]}
    )
    backend = _RecordingChannelsBackend()
    config = QueueConfig(
        events=QueueEventsConfig(
            channels=backend,
            delivery=EventDeliveryConfig(
                max_payload_bytes=_estimate_wrapped_event_bytes(single_item_event) + 16,
                payload_size_estimator=_estimate_wrapped_event_bytes,
            ),
        )
    )

    async with create_event_producer(config) as producer:
        await producer.task("task-1").event("task.event", payload={"batch": True, "count": len(items), "items": items})

    assert len(backend.published) > 1
    decoded = [QueueEvent.from_json(payload) for payload, _ in backend.published]
    assert [item for chunk in decoded for item in chunk.payload["items"]] == items


class _RecordingChannelsBackend:
    def __init__(self) -> None:
        self.published: "list[tuple[bytes | str, tuple[str, ...]]]" = []

    def publish(self, data: "bytes | str", channels: "Sequence[str]") -> None:
        self.published.append((data, tuple(channels)))


def _estimate_wrapped_event_bytes(event: "QueueEvent") -> "int":
    payload = {
        "payload": {"data_b64": base64.b64encode(event.to_json()).decode("ascii")},
        "metadata": {"transport": "test"},
    }
    return len(msgspec.json.encode(payload))

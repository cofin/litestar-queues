from typing import TYPE_CHECKING, ClassVar

import pytest

from litestar_queues import EventConfig, QueueConfig
from litestar_queues.events import EventBufferConfig, QueueChannels, QueueEvent
from litestar_queues.events.sqlspec import SQLSpecQueueEventSink

if TYPE_CHECKING:
    from collections.abc import Sequence

    from litestar_queues.backends import BaseQueueBackend

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
            await producer.channel("imports:acme").publish("x", immediate=True)


async def test_factory_sqlspec_transport_publishes_through_event_channel() -> None:
    from litestar_queues.events import create_event_producer

    event_channel = _RecordingSqlSpecEventChannel()
    config = QueueConfig(
        queue_backend=_SqlSpecBackendConfig(event_channel=event_channel),
        event=EventConfig(buffer=EventBufferConfig(enabled=False)),
    )

    async with create_event_producer(config, transport="sqlspec") as producer:
        await producer.task("task-1").progress(current=1, total=2, immediate=True)

    assert event_channel.shutdown_count == 0
    [(channel, payload, metadata)] = event_channel.published
    assert channel == QueueChannels.task("task-1")
    assert payload["type"] == "task.progress"
    assert payload["taskId"] == "task-1"
    assert payload["progressCurrent"] == 1
    assert metadata == {"event_type": "task.progress", "queue_event_id": payload["id"], "queue_event_scope": "task"}


async def test_factory_sqlspec_transport_drains_buffer_before_close() -> None:
    from litestar_queues.events import create_event_producer

    event_channel = _RecordingSqlSpecEventChannel()
    config = QueueConfig(
        queue_backend=_SqlSpecBackendConfig(event_channel=event_channel),
        event=EventConfig(buffer=EventBufferConfig(buffer_size=10, flush_interval=60)),
    )

    async with create_event_producer(config, transport="sqlspec") as producer:
        await producer.task("task-1").log("buffered")
        await producer.task("task-1").progress(current=1, total=2)
        assert event_channel.published == []

    assert [payload["type"] for _, payload, _ in event_channel.published] == ["task.log", "task.progress"]
    assert event_channel.publish_calls == 0
    assert event_channel.publish_many_calls == 1


async def test_sqlspec_sink_publish_many_flattens_ordered_events_into_one_call() -> None:
    event_channel = _RecordingSqlSpecEventChannel()
    sink = SQLSpecQueueEventSink(_SqlSpecBackendConfig(event_channel=event_channel))
    first = QueueEvent(type="task.progress", scope="task", task_id="task-1")
    second = QueueEvent(type="task.log", scope="task", task_id="task-2")

    await sink.publish_many(((first, ("task-1", "global")), (second, ("task-2",))))

    assert event_channel.publish_calls == 0
    assert event_channel.publish_many_calls == 1
    assert [(channel, payload["type"]) for channel, payload, _ in event_channel.published] == [
        ("task-1", "task.progress"),
        ("global", "task.progress"),
        ("task-2", "task.log"),
    ]


async def test_sqlspec_sink_publish_many_supports_sync_event_channel() -> None:
    event_channel = _RecordingSyncSqlSpecEventChannel()
    sink = SQLSpecQueueEventSink(_SqlSpecBackendConfig(event_channel=event_channel))
    event = QueueEvent(type="task.log", scope="task", task_id="task-1")

    await sink.publish_many(((event, ("task-1",)),))

    assert event_channel.publish_many_calls == 1
    assert [(channel, payload["type"]) for channel, payload, _ in event_channel.published] == [("task-1", "task.log")]


async def test_sqlspec_sink_publish_many_empty_batch_does_not_create_channel() -> None:
    sqlspec = _RecordingSQLSpec()
    sink = SQLSpecQueueEventSink(_SqlSpecBackendConfig(sqlspec=sqlspec, config=_RecordingSQLSpecConfig()))

    await sink.publish_many(())

    assert sqlspec.channel_calls == 0


async def test_sqlspec_sink_publish_many_propagates_batch_failure() -> None:
    event_channel = _FailingSqlSpecEventChannel()
    sink = SQLSpecQueueEventSink(_SqlSpecBackendConfig(event_channel=event_channel))
    event = QueueEvent(type="task.log", scope="task", task_id="task-1")

    with pytest.raises(RuntimeError, match="batch publish failed"):
        await sink.publish_many(((event, ("task-1",)),))

    assert event_channel.publish_calls == 0
    assert event_channel.publish_many_calls == 1


async def test_factory_sqlspec_transport_builds_channel_lazily_and_closes_owned_channel() -> None:
    from litestar_queues.events import create_event_producer

    sqlspec = _RecordingSQLSpec()
    config = QueueConfig(
        queue_backend=_SqlSpecBackendConfig(sqlspec=sqlspec, config=_RecordingSQLSpecConfig()),
        event=EventConfig(buffer=EventBufferConfig(enabled=False)),
    )
    external = create_event_producer(config, transport="sqlspec")

    assert sqlspec.channel_calls == 0
    async with external as producer:
        assert sqlspec.channel_calls == 0
        await producer.channel("imports:acme").publish("import.note", immediate=True)
        assert sqlspec.channel_calls == 1

    assert sqlspec.created_event_channel.shutdown_count == 1
    assert sqlspec.close_all_pools_count == 0


def test_create_event_producer_import_does_not_load_sqlspec() -> None:
    import subprocess
    import sys

    code = """
import sys
from litestar_queues.events import create_event_producer
raise SystemExit(1 if "sqlspec" in sys.modules else 0)
"""

    result = subprocess.run([sys.executable, "-c", code], check=False, capture_output=True, text=True)

    assert result.returncode == 0, result.stdout


async def test_manual_aclose() -> None:
    from litestar_queues.events import create_event_producer

    backend = _RecordingChannelsBackend()
    external = create_event_producer(QueueConfig(event=EventConfig(channels_backend=backend)))
    producer = await external.__aenter__()
    await producer.channel("imports:acme").publish("x")
    await external.aclose()

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
    def get_queue_backend(self) -> "BaseQueueBackend":
        msg = "queue backend must not open"
        raise AssertionError(msg)


class _SqlSpecBackendConfig:
    backend_name: "ClassVar[str]" = "sqlspec"

    def __init__(
        self,
        *,
        event_channel: "object | None" = None,
        sqlspec: "_RecordingSQLSpec | None" = None,
        config: "_RecordingSQLSpecConfig | None" = None,
    ) -> None:
        self.event_channel = event_channel
        self.sqlspec = sqlspec
        self.config = config
        self.event_backend = "poll_queue"
        self.event_queue_table = "litestar_queue_events"
        self.event_poll_interval = None
        self.event_settings: "dict[str, object]" = {}
        self.notify_transport = None


class _RecordingSQLSpecConfig:
    def __init__(self) -> None:
        self.extension_config: "dict[str, dict[str, object]]" = {}
        self.migration_config: "dict[str, object]" = {}

    def set_migration_config(self, config: "dict[str, object]") -> None:
        self.migration_config = config


class _RecordingSQLSpec:
    def __init__(self) -> None:
        self.channel_calls = 0
        self.close_all_pools_count = 0
        self.created_event_channel = _RecordingSqlSpecEventChannel()

    def event_channel(self, config: "_RecordingSQLSpecConfig") -> "_RecordingSqlSpecEventChannel":
        self.channel_calls += 1
        return self.created_event_channel

    async def close_all_pools(self) -> None:
        self.close_all_pools_count += 1


class _RecordingSqlSpecEventChannel:
    def __init__(self) -> None:
        self.published: "list[tuple[str, dict[str, object], dict[str, object] | None]]" = []
        self.publish_calls = 0
        self.publish_many_calls = 0
        self.shutdown_count = 0

    async def publish(
        self, channel: "str", payload: "dict[str, object]", metadata: "dict[str, object] | None" = None
    ) -> "str":
        self.publish_calls += 1
        event_id = f"event-{len(self.published) + 1}"
        self.published.append((channel, payload, metadata))
        return event_id

    async def publish_many(
        self, events: "Sequence[tuple[str, dict[str, object], dict[str, object] | None]]"
    ) -> "list[str]":
        self.publish_many_calls += 1
        event_ids = [f"event-{len(self.published) + index + 1}" for index in range(len(events))]
        self.published.extend(events)
        return event_ids

    async def shutdown(self) -> None:
        self.shutdown_count += 1


class _FailingSqlSpecEventChannel(_RecordingSqlSpecEventChannel):
    async def publish_many(
        self, events: "Sequence[tuple[str, dict[str, object], dict[str, object] | None]]"
    ) -> "list[str]":
        self.publish_many_calls += 1
        msg = "batch publish failed"
        raise RuntimeError(msg)


class _RecordingSyncSqlSpecEventChannel:
    def __init__(self) -> None:
        self.published: "list[tuple[str, dict[str, object], dict[str, object] | None]]" = []
        self.publish_many_calls = 0

    def publish_many(self, events: "Sequence[tuple[str, dict[str, object], dict[str, object] | None]]") -> "list[str]":
        self.publish_many_calls += 1
        self.published.extend(events)
        return [f"event-{index + 1}" for index in range(len(events))]

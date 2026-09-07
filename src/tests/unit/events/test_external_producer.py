from typing import TYPE_CHECKING

import pytest

from litestar_queues import EventDeliveryConfig, QueueConfig, WorkerConfig
from litestar_queues.backends.sqlspec.event_sink import SQLSpecQueueEventSink
from litestar_queues.events import EventBufferConfig, QueueChannels, QueueEvent, QueueEventsConfig

if TYPE_CHECKING:
    from collections.abc import Sequence

    from litestar_queues.backends import BaseQueueBackend
    from litestar_queues.events.producer import _ExternalProducer

pytestmark = pytest.mark.anyio


async def test_factory_opens_and_closes_channels_backend() -> None:
    from litestar_queues.events import create_event_producer

    backend = _RecordingChannelsBackend()

    async with create_event_producer(
        QueueConfig(
            worker=WorkerConfig(placement="external"),
            queue_backend="memory",
            events=QueueEventsConfig(channels=backend, delivery=EventDeliveryConfig()),
        )
    ) as producer:
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

    config = _ExplodingBackendConfig(
        events=QueueEventsConfig(channels=_RecordingChannelsBackend(), delivery=EventDeliveryConfig())
    )

    async with create_event_producer(config) as producer:
        await producer.channel("imports:acme").publish("x")


async def test_factory_tolerates_backend_without_open_close() -> None:
    from litestar_queues.events import create_event_producer

    backend = _PublishOnlyChannelsBackend()

    async with create_event_producer(
        QueueConfig(
            worker=WorkerConfig(placement="external"),
            queue_backend="memory",
            events=QueueEventsConfig(channels=backend, delivery=EventDeliveryConfig()),
        )
    ) as producer:
        await producer.channel("imports:acme").publish("x")

    assert len(backend.published) == 1


async def test_factory_strict_propagates() -> None:
    from litestar_queues.events import create_event_producer

    async with create_event_producer(
        QueueConfig(
            worker=WorkerConfig(placement="external"),
            queue_backend="memory",
            events=QueueEventsConfig(channels=_FailingChannelsBackend(), delivery=EventDeliveryConfig(strict=True)),
        )
    ) as producer:
        with pytest.raises(RuntimeError, match="publish failed"):
            await producer.channel("imports:acme").publish("x", immediate=True)


async def test_factory_explicit_sqlspec_sink_publishes_through_event_channel() -> None:
    from litestar_queues.events import create_event_producer

    event_channel = _RecordingSqlSpecEventChannel()
    sink = SQLSpecQueueEventSink(event_channel)
    config = QueueConfig(
        worker=WorkerConfig(placement="external"),
        queue_backend="memory",
        events=QueueEventsConfig(delivery=EventDeliveryConfig(buffer=None, sinks=(sink,))),
    )

    async with create_event_producer(config) as producer:
        await producer.task("task-1").progress(current=1, total=2, immediate=True)

    assert event_channel.shutdown_count == 0
    [(channel, payload, metadata)] = event_channel.published
    assert channel == QueueChannels.task("task-1")
    assert payload["type"] == "task.progress"
    assert payload["taskId"] == "task-1"
    assert payload["progressCurrent"] == 1
    assert metadata == {"event_type": "task.progress", "queue_event_id": payload["id"], "queue_event_scope": "task"}


async def test_factory_explicit_sqlspec_sink_drains_buffer_before_close() -> None:
    from litestar_queues.events import create_event_producer

    event_channel = _RecordingSqlSpecEventChannel()
    sink = SQLSpecQueueEventSink(event_channel)
    config = QueueConfig(
        worker=WorkerConfig(placement="external"),
        queue_backend="memory",
        events=QueueEventsConfig(
            delivery=EventDeliveryConfig(buffer=EventBufferConfig(batch_size=10, flush_interval=60), sinks=(sink,))
        ),
    )

    async with create_event_producer(config) as producer:
        await producer.task("task-1").log("buffered")
        await producer.task("task-1").progress(current=1, total=2)
        assert event_channel.published == []

    assert [payload["type"] for _, payload, _ in event_channel.published] == ["task.log", "task.progress"]
    assert event_channel.publish_calls == 0
    assert event_channel.publish_many_calls == 1


async def test_sqlspec_sink_publish_many_flattens_ordered_events_into_one_call() -> None:
    event_channel = _RecordingSqlSpecEventChannel()
    sink = SQLSpecQueueEventSink(event_channel)
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
    sink = SQLSpecQueueEventSink(event_channel)
    event = QueueEvent(type="task.log", scope="task", task_id="task-1")

    await sink.publish_many(((event, ("task-1",)),))

    assert event_channel.publish_many_calls == 1
    assert [(channel, payload["type"]) for channel, payload, _ in event_channel.published] == [("task-1", "task.log")]


async def test_sqlspec_sink_publish_many_empty_batch_does_not_create_channel() -> None:
    sqlspec = _RecordingSQLSpec()
    sink = SQLSpecQueueEventSink(sqlspec=sqlspec, sqlspec_config=_RecordingSQLSpecConfig())

    await sink.publish_many(())

    assert sqlspec.channel_calls == 0


async def test_sqlspec_sink_publish_many_propagates_batch_failure() -> None:
    event_channel = _FailingSqlSpecEventChannel()
    sink = SQLSpecQueueEventSink(event_channel)
    event = QueueEvent(type="task.log", scope="task", task_id="task-1")

    with pytest.raises(RuntimeError, match="batch publish failed"):
        await sink.publish_many(((event, ("task-1",)),))

    assert event_channel.publish_calls == 0
    assert event_channel.publish_many_calls == 1


async def test_factory_explicit_sqlspec_sink_builds_channel_lazily_and_closes_owned_channel() -> None:
    from litestar_queues.events import create_event_producer

    sqlspec = _RecordingSQLSpec()
    sink = SQLSpecQueueEventSink(
        sqlspec=sqlspec,
        sqlspec_config=_RecordingSQLSpecConfig(),
        settings={"backend": "poll_queue", "queue_table": "queue_events"},
    )
    config = QueueConfig(
        worker=WorkerConfig(placement="external"),
        queue_backend="memory",
        events=QueueEventsConfig(delivery=EventDeliveryConfig(buffer=None, sinks=(sink,))),
    )
    external = create_event_producer(config)

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
    external = create_event_producer(
        QueueConfig(
            worker=WorkerConfig(placement="external"),
            queue_backend="memory",
            events=QueueEventsConfig(channels=backend, delivery=EventDeliveryConfig()),
        )
    )
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


class _LifecycleResource:
    def __init__(
        self,
        name: "str",
        calls: "list[str]",
        *,
        open_error: "BaseException | None" = None,
        close_error: "BaseException | None" = None,
    ) -> None:
        self.name = name
        self.calls = calls
        self.open_error = open_error
        self.close_error = close_error

    async def open(self) -> None:
        self.calls.append(f"{self.name}.open")
        if self.open_error is not None:
            raise self.open_error

    async def close(self) -> None:
        self.calls.append(f"{self.name}.close")
        if self.close_error is not None:
            raise self.close_error

    async def publish(self, event: "QueueEvent", *, channels: "Sequence[str]") -> None:
        self.calls.append(f"{self.name}.publish")


def _external_for(resources: "Sequence[_LifecycleResource]") -> "_ExternalProducer":
    from litestar_queues.events import create_event_producer

    return create_event_producer(
        _ExplodingBackendConfig(
            events=QueueEventsConfig(delivery=EventDeliveryConfig(buffer=None, sinks=tuple(resources)))
        )
    )


@pytest.mark.parametrize("position", [0, 1, 2])
@pytest.mark.parametrize("cancel", [False, True])
async def test_partial_open_unwinds_only_successfully_acquired_resources(position: int, cancel: bool) -> None:
    import asyncio

    calls: "list[str]" = []
    original = asyncio.CancelledError() if cancel else RuntimeError("original open failure")
    resources = [
        _LifecycleResource(str(index), calls, close_error=ValueError("secondary close failure")) for index in range(3)
    ]
    resources[position].open_error = original
    external = _external_for(resources)
    with pytest.raises(type(original)) as raised:
        await external.__aenter__()
    assert raised.value is original
    expected = [f"{index}.open" for index in range(position + 1)] + [
        f"{index}.close" for index in reversed(range(position))
    ]
    assert calls == expected
    await external.aclose()
    assert calls == expected


@pytest.mark.parametrize("position", [0, 1, 2])
async def test_close_failure_attempts_all_acquired_resources_once(position: int) -> None:
    calls: "list[str]" = []
    original = RuntimeError("close failure")
    resources = [_LifecycleResource(str(index), calls) for index in range(3)]
    resources[position].close_error = original
    external = _external_for(resources)
    await external.__aenter__()
    with pytest.raises(RuntimeError) as raised:
        await external.aclose()
    assert raised.value is original
    assert calls == ["0.open", "1.open", "2.open", "2.close", "1.close", "0.close"]
    await external.aclose()
    assert len(calls) == 6


@pytest.mark.parametrize("cancel", [False, True])
async def test_context_body_error_wins_over_all_cleanup_errors(cancel: bool) -> None:
    import asyncio

    calls: "list[str]" = []
    original = asyncio.CancelledError() if cancel else LookupError("body failure")
    resources = [_LifecycleResource(str(index), calls, close_error=RuntimeError("close failure")) for index in range(3)]
    external = _external_for(resources)
    with pytest.raises(type(original)) as raised:
        async with external:
            raise original
    assert raised.value is original
    assert calls == ["0.open", "1.open", "2.open", "2.close", "1.close", "0.close"]
    await external.aclose()


async def test_duplicate_resource_identity_is_acquired_and_closed_once() -> None:
    calls: "list[str]" = []
    resource = _LifecycleResource("same", calls)
    async with _external_for((resource, resource)) as producer:
        await producer.task("task").log("one", immediate=True)
    assert calls == ["same.open", "same.publish", "same.publish", "same.close"]


async def test_close_only_resource_remains_unowned() -> None:
    from types import SimpleNamespace
    from typing import Any, cast
    from unittest.mock import AsyncMock

    resource = SimpleNamespace(publish=AsyncMock(), close=AsyncMock())
    external = _external_for((cast("Any", resource),))
    async with external:
        pass
    resource.close.assert_not_awaited()


async def test_buffer_stop_failure_still_attempts_every_close(monkeypatch: pytest.MonkeyPatch) -> None:
    import asyncio

    from litestar_queues.events.publisher import QueueEventPublisher

    calls: "list[str]" = []
    first = RuntimeError("buffer stop failure")
    cancellation = asyncio.CancelledError()
    resources = [_LifecycleResource(str(index), calls) for index in range(3)]
    resources[1].close_error = cancellation
    resources[2].close_error = ValueError("first close failure")

    async def stop(_publisher: QueueEventPublisher) -> None:
        calls.append("buffer.stop")
        raise first

    monkeypatch.setattr(QueueEventPublisher, "stop_buffer", stop)
    external = _external_for(resources)
    await external.__aenter__()
    with pytest.raises(asyncio.CancelledError) as raised:
        await external.aclose()
    assert raised.value is cancellation
    assert calls == ["0.open", "1.open", "2.open", "buffer.stop", "2.close", "1.close", "0.close"]
    await external.aclose()


async def test_failed_publisher_start_unwinds_acquired_resources(monkeypatch: pytest.MonkeyPatch) -> None:
    from litestar_queues.events.publisher import QueueEventPublisher

    calls: "list[str]" = []
    original = RuntimeError("buffer start failure")

    def start(_publisher: QueueEventPublisher) -> None:
        raise original

    monkeypatch.setattr(QueueEventPublisher, "start_buffer", start)
    external = _external_for([_LifecycleResource(str(index), calls) for index in range(3)])
    with pytest.raises(RuntimeError) as raised:
        await external.__aenter__()
    assert raised.value is original
    assert calls == ["0.open", "1.open", "2.open", "2.close", "1.close", "0.close"]
    await external.aclose()


async def test_actual_cancellation_during_close_attempts_remaining_resources() -> None:
    import asyncio

    calls: "list[str]" = []
    waiting = asyncio.Event()

    class BlockingResource(_LifecycleResource):
        async def close(self) -> None:
            self.calls.append(f"{self.name}.close")
            waiting.set()
            await asyncio.Event().wait()

    resources = [
        _LifecycleResource("0", calls),
        BlockingResource("1", calls),
        _LifecycleResource("2", calls, close_error=RuntimeError("earlier close failed")),
    ]
    external = _external_for(resources)
    await external.__aenter__()
    closing = asyncio.create_task(external.aclose())
    try:
        await asyncio.wait_for(waiting.wait(), timeout=1)
        closing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(closing, timeout=1)
        assert calls == ["0.open", "1.open", "2.open", "2.close", "1.close", "0.close"]
        await external.aclose()
        assert len(calls) == 6
    finally:
        if not closing.done():
            closing.cancel()
        await asyncio.gather(closing, return_exceptions=True)


async def test_secondary_logging_failure_cannot_replace_body_error(monkeypatch: pytest.MonkeyPatch) -> None:
    import litestar_queues.events.producer as producer_module

    calls: "list[str]" = []
    original = LookupError("body failed")

    def fail_logging(*args: object, **kwargs: object) -> None:
        message = "logging failed"
        raise RuntimeError(message)

    monkeypatch.setattr(producer_module.logger, "warning", fail_logging)
    external = _external_for([
        _LifecycleResource(str(index), calls, close_error=ValueError("close failed")) for index in range(3)
    ])
    with pytest.raises(LookupError) as raised:
        async with external:
            raise original
    assert raised.value is original
    assert calls == ["0.open", "1.open", "2.open", "2.close", "1.close", "0.close"]
    await external.aclose()


@pytest.mark.parametrize("outer_cleanup", [False, True])
async def test_live_callback_cannot_close_its_own_producer_during_outer_cleanup(outer_cleanup: bool) -> None:
    from litestar_queues.events import create_event_producer
    from litestar_queues.exceptions import QueueConfigurationError

    calls: "list[str]" = []

    class ReentrantResource(_LifecycleResource):
        async def publish(self, event: "QueueEvent", *, channels: "Sequence[str]") -> None:
            calls.append("sink.publish")
            with pytest.raises(QueueConfigurationError, match="active event release"):
                await external.aclose()
            with pytest.raises(QueueConfigurationError, match="active event release"):
                await external.__aenter__()
            assert calls == ["sink.open", "sink.publish"]

    sink = ReentrantResource("sink", calls)
    external = create_event_producer(
        _ExplodingBackendConfig(
            events=QueueEventsConfig(
                delivery=EventDeliveryConfig(
                    sinks=(sink,), strict=True, buffer=EventBufferConfig(batch_size=10, flush_interval=60)
                )
            )
        )
    )
    producer = await external.__aenter__()
    await producer.task("task").log("buffered")
    if not outer_cleanup:
        assert external._publisher is not None
        await external._publisher.flush_buffer()
    await external.aclose()
    assert calls == ["sink.open", "sink.publish", "sink.close"]
    await external.aclose()

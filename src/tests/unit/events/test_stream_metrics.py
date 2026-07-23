import asyncio
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

import pytest
from litestar import Litestar
from litestar.exceptions import WebSocketException
from litestar.routes import WebSocketRoute

from litestar_queues import QueuePlugin, WorkerConfig
from litestar_queues.config import QueueConfig
from litestar_queues.events import EventDeliveryConfig, EventStreamConfig, QueueChannels, QueueEvent, QueueEventsConfig
from litestar_queues.events.streaming import build_stream_router
from litestar_queues.observability import ObservabilityConfig

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Mapping, Sequence

    from litestar.handlers.websocket_handlers import WebsocketRouteHandler

pytestmark = pytest.mark.anyio


async def test_plugin_startup_publishes_observability_runtime_on_state(monkeypatch: "pytest.MonkeyPatch") -> None:
    runtime = _FakeObservabilityRuntime()

    def create_runtime(
        config: "ObservabilityConfig | None", *, app: Litestar | None = None
    ) -> "_FakeObservabilityRuntime":
        assert config is not None
        assert app is not None
        return runtime

    monkeypatch.setattr("litestar_queues.observability.create_observability_runtime", create_runtime)
    plugin = QueuePlugin(
        QueueConfig(observability=ObservabilityConfig(enable_otel=False), worker=WorkerConfig(run_in_app=False))
    )
    app = Litestar(plugins=[plugin])

    async with plugin._lifespan(app):
        assert app.state["queue_observability_runtime"] is runtime


async def test_websocket_stream_metrics_recorded_with_bounded_labels() -> None:
    event = QueueEvent(
        type="task.progress",
        scope="task",
        task_id="task-1",
        event_key="progress-1",
        payload={"tenant_id": "tenant-1", "value": 42},
    )
    runtime = _FakeObservabilityRuntime()
    channels = _FakeChannelsPlugin([event.to_json(), event.to_json()])
    socket = _RecordingSocket(runtime=runtime)
    router = build_stream_router(
        QueueConfig(
            events=QueueEventsConfig(channels=channels, delivery=EventDeliveryConfig()),
            observability=ObservabilityConfig(enable_otel=False),
        ),
        EventStreamConfig(scopes={"task"}, heartbeat_interval=0),
    )

    await _stream_handler(router, "/tasks/{task_id:str}").fn(socket, task_id="task-1")

    assert channels.subscribed_channels == [QueueChannels.task("task-1")]
    assert socket.sent_json == [event.to_dict()]
    assert ("litestar_queues.stream.connections", 1, {"scope": "task"}) in runtime.counters
    assert ("litestar_queues.stream.events_sent", 1, {"scope": "task"}) in runtime.counters
    assert ("litestar_queues.stream.dedup_drops", 1, {"scope": "task"}) in runtime.counters
    assert ("litestar_queues.stream.active", 1, {"scope": "task"}) in runtime.gauges
    assert ("litestar_queues.stream.active", -1, {"scope": "task"}) in runtime.gauges
    assert runtime.durations[0][0] == "litestar_queues.stream.connection.duration"
    _assert_bounded_labels(runtime)


async def test_websocket_stream_metrics_record_heartbeats() -> None:
    runtime = _FakeObservabilityRuntime()
    channels = _FakeChannelsPlugin([], delay_before_close=0.01)
    socket = _RecordingSocket(runtime=runtime)
    router = build_stream_router(
        QueueConfig(
            events=QueueEventsConfig(channels=channels, delivery=EventDeliveryConfig()),
            observability=ObservabilityConfig(enable_otel=False),
        ),
        EventStreamConfig(scopes={"task"}, heartbeat_interval=0.001),
    )

    await _stream_handler(router, "/tasks/{task_id:str}").fn(socket, task_id="task-1")

    assert {"type": "ping"} in socket.sent_json
    assert ("litestar_queues.stream.heartbeats", 1, {"scope": "task"}) in runtime.counters
    _assert_bounded_labels(runtime)


async def test_stream_metrics_noop_without_observability_runtime() -> None:
    event = QueueEvent(type="task.progress", scope="task", task_id="task-1")
    channels = _FakeChannelsPlugin([event.to_json()])
    socket = _RecordingSocket()
    router = build_stream_router(
        QueueConfig(events=QueueEventsConfig(channels=channels, delivery=EventDeliveryConfig())),
        EventStreamConfig(scopes={"task"}, heartbeat_interval=0),
    )

    await _stream_handler(router, "/tasks/{task_id:str}").fn(socket, task_id="task-1")

    assert socket.sent_json == [event.to_dict()]


async def test_authorizer_denial_records_authz_reason() -> None:
    runtime = _FakeObservabilityRuntime()
    channels = _FakeChannelsPlugin([])
    socket = _RecordingSocket(runtime=runtime)
    router = build_stream_router(
        QueueConfig(
            events=QueueEventsConfig(channels=channels, delivery=EventDeliveryConfig()),
            observability=ObservabilityConfig(enable_otel=False),
        ),
        EventStreamConfig(scopes={"task"}, channel_authorizer=lambda *_: False, heartbeat_interval=0),
    )

    with pytest.raises(WebSocketException) as exc_info:
        await _stream_handler(router, "/tasks/{task_id:str}").fn(socket, task_id="task-1")

    assert exc_info.value.code == 4003
    assert not socket.accepted
    assert channels.subscribed_channels is None
    assert runtime.counters == [("litestar_queues.stream.auth_denials", 1, {"scope": "task", "reason": "authz"})]
    _assert_bounded_labels(runtime)


class _FakeSubscriber:
    def __init__(self, events: "Sequence[bytes]", *, delay_before_close: float = 0.0) -> None:
        self._events = events
        self._delay_before_close = delay_before_close

    async def iter_events(self) -> "AsyncIterator[bytes]":
        if self._delay_before_close:
            await asyncio.sleep(self._delay_before_close)
        for event in self._events:
            yield event


class _FakeChannelsPlugin:
    def __init__(self, events: "Sequence[bytes]", *, delay_before_close: float = 0.0) -> None:
        self._events = events
        self._delay_before_close = delay_before_close
        self.subscribed_channels: "list[str] | None" = None

    @asynccontextmanager
    async def start_subscription(
        self, channels: "Sequence[str]", history: int | None = None
    ) -> "AsyncIterator[_FakeSubscriber]":
        self.subscribed_channels = list(channels)
        yield _FakeSubscriber(self._events, delay_before_close=self._delay_before_close)


class _RecordingSocket:
    def __init__(self, runtime: "_FakeObservabilityRuntime | None" = None) -> None:
        self.accepted = False
        self.sent_json: list[dict[str, object]] = []
        self.app = _FakeApp(runtime)

    async def accept(self) -> None:
        self.accepted = True

    async def send_json(self, data: dict[str, object]) -> None:
        self.sent_json.append(data)


class _FakeApp:
    def __init__(self, runtime: "_FakeObservabilityRuntime | None") -> None:
        self.state: dict[str, object] = {}
        if runtime is not None:
            self.state["queue_observability_runtime"] = runtime


class _FakeObservabilityRuntime:
    enabled = True

    def __init__(self) -> None:
        self.counters: "list[tuple[str, int, dict[str, str]]]" = []
        self.gauges: "list[tuple[str, int, dict[str, str]]]" = []
        self.durations: "list[tuple[str, float, dict[str, str]]]" = []

    def record_counter(self, name: "str", value: "int" = 1, *, attributes: "Mapping[str, str]") -> None:
        self.counters.append((name, value, dict(attributes)))

    def record_gauge_delta(self, name: "str", delta: "int" = 1, *, attributes: "Mapping[str, str]") -> None:
        self.gauges.append((name, delta, dict(attributes)))

    def record_duration(self, name: "str", seconds: "float", *, attributes: "Mapping[str, str]") -> None:
        self.durations.append((name, seconds, dict(attributes)))


def _stream_handler(router: Any, path: str) -> "WebsocketRouteHandler":
    app = Litestar(route_handlers=[router], openapi_config=None)
    for route in app.routes:
        if isinstance(route, WebSocketRoute) and route.path.endswith(path):
            return route.route_handler
    msg = f"No stream handler registered for {path!r}."
    raise AssertionError(msg)


def _assert_bounded_labels(runtime: "_FakeObservabilityRuntime") -> None:
    metric_attributes: list[dict[str, str]] = []
    for _name, _value, attributes in runtime.counters:
        metric_attributes.append(attributes)
    for _name, _value, attributes in runtime.gauges:
        metric_attributes.append(attributes)
    for _name, _duration, attributes in runtime.durations:
        metric_attributes.append(attributes)
    for attributes in metric_attributes:
        assert set(attributes) <= {"scope", "reason"}
        assert "task-1" not in attributes.values()
        assert "tenant-1" not in attributes.values()

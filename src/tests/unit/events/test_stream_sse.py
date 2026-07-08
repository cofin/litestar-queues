import asyncio
import json
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

import pytest
from litestar import Litestar
from litestar.connection import ASGIConnection
from litestar.exceptions import PermissionDeniedException
from litestar.handlers.base import BaseRouteHandler
from litestar.routes import HTTPRoute
from litestar.testing import create_test_client

from litestar_queues.config import QueueConfig
from litestar_queues.events import EventConfig, EventStreamConfig, QueueChannels, QueueEvent
from litestar_queues.events.streaming import build_stream_router

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Sequence

    from litestar.handlers.http_handlers import HTTPRouteHandler

pytestmark = pytest.mark.anyio


async def test_sse_endpoint_registered_for_scopes() -> None:
    router = build_stream_router(QueueConfig(), EventStreamConfig(scopes={"task"}))

    assert _stream_paths(router) == {"/queues/events/tasks/{task_id:str}", "/queues/events/sse/tasks/{task_id:str}"}


async def test_sse_disabled_registers_no_sse_routes() -> None:
    router = build_stream_router(QueueConfig(), EventStreamConfig(scopes={"task"}, sse=False))

    assert _stream_paths(router) == {"/queues/events/tasks/{task_id:str}"}


async def test_sse_client_receives_event_frames() -> None:
    event = QueueEvent(type="task.progress", scope="task", task_id="task-1", message="working")
    channels = _FakeChannelsPlugin([event.to_json()])
    router = build_stream_router(
        QueueConfig(event=EventConfig(channels_backend=channels)),
        EventStreamConfig(scopes={"task"}, heartbeat_interval=0),
    )

    response = await _sse_handler(router, "/sse/tasks/{task_id:str}").fn(_Connection(), task_id="task-1")
    frames = await _response_frames(response)

    assert channels.subscribed_channels == [QueueChannels.task("task-1")]
    assert b"event: task.progress" in frames[0]
    data = _frame_data(frames[0])
    assert json.loads(data) == event.to_dict()


async def test_sse_authorizer_denies_before_subscribe() -> None:
    channels = _FakeChannelsPlugin([])
    router = build_stream_router(
        QueueConfig(event=EventConfig(channels_backend=channels)),
        EventStreamConfig(scopes={"task"}, channel_authorizer=lambda *_: False),
    )

    with pytest.raises(PermissionDeniedException):
        await _sse_handler(router, "/sse/tasks/{task_id:str}").fn(_Connection(), task_id="task-1")

    assert channels.subscribed_channels is None


async def test_sse_task_route_serves_event_stream_content_type() -> None:
    event = QueueEvent(type="task.progress", scope="task", task_id="task-1", message="working")
    channels = _FakeChannelsPlugin([event.to_json()])
    router = build_stream_router(
        QueueConfig(event=EventConfig(channels_backend=channels)),
        EventStreamConfig(scopes={"task"}, heartbeat_interval=0),
    )

    with create_test_client(route_handlers=[router], openapi_config=None) as client:
        response = client.get("/queues/events/sse/tasks/task-1")

    assert response.headers["content-type"].startswith("text/event-stream")


async def test_sse_custom_route_serves_event_stream_content_type() -> None:
    event = QueueEvent(type="task.progress", scope="custom", message="working")
    channels = _FakeChannelsPlugin([event.to_json()])
    router = build_stream_router(
        QueueConfig(event=EventConfig(channels_backend=channels)),
        EventStreamConfig(scopes={"custom"}, heartbeat_interval=0),
    )

    with create_test_client(route_handlers=[router], openapi_config=None) as client:
        response = client.get("/queues/events/sse/custom/key-1")

    assert response.headers["content-type"].startswith("text/event-stream")


async def test_sse_guard_denies_before_authorizer() -> None:
    calls: list[object] = []

    def authorize(*args: object) -> bool:
        calls.append(args)
        return True

    router = build_stream_router(
        QueueConfig(), EventStreamConfig(scopes={"task"}, guards=[_deny_guard], channel_authorizer=authorize)
    )

    with create_test_client(route_handlers=[router], openapi_config=None) as client:
        response = client.get("/queues/events/sse/tasks/task-1")

    assert response.status_code == 403
    assert calls == []


async def test_sse_keepalive_comment_and_dedup() -> None:
    first = QueueEvent(type="task.progress", scope="task", id="evt-1", task_id="task-1", event_key="same")
    duplicate = QueueEvent(type="task.progress", scope="task", id="evt-2", task_id="task-1", event_key="same")
    second = QueueEvent(type="task.log", scope="task", id="evt-3", task_id="task-1")
    channels = _FakeChannelsPlugin([first.to_json(), duplicate.to_json(), second.to_json()], delay_between_events=0.03)
    router = build_stream_router(
        QueueConfig(event=EventConfig(channels_backend=channels)),
        EventStreamConfig(scopes={"task"}, heartbeat_interval=0.005),
    )

    response = await _sse_handler(router, "/sse/tasks/{task_id:str}").fn(_Connection(), task_id="task-1")
    frames = await _response_frames(response)

    data_frames = [frame for frame in frames if frame.startswith(b"event:")]
    comment_frames = [frame for frame in frames if frame.startswith(b":")]
    assert [json.loads(_frame_data(frame))["id"] for frame in data_frames] == ["evt-1", "evt-3"]
    assert comment_frames


class _FakeSubscriber:
    def __init__(self, events: "Sequence[bytes]", *, delay_between_events: float = 0.0) -> None:
        self._events = events
        self._delay_between_events = delay_between_events

    async def iter_events(self) -> "AsyncIterator[bytes]":
        for index, event in enumerate(self._events):
            if index:
                await asyncio.sleep(self._delay_between_events)
            yield event


class _FakeChannelsPlugin:
    def __init__(self, events: "Sequence[bytes]", *, delay_between_events: float = 0.0) -> None:
        self._events = events
        self._delay_between_events = delay_between_events
        self.subscribed_channels: "list[str] | None" = None

    @asynccontextmanager
    async def start_subscription(
        self, channels: "Sequence[str]", history: int | None = None
    ) -> "AsyncIterator[_FakeSubscriber]":
        self.subscribed_channels = list(channels)
        yield _FakeSubscriber(self._events, delay_between_events=self._delay_between_events)


class _Connection:
    pass


def _stream_paths(router: Any) -> "set[str]":
    app = Litestar(route_handlers=[router], openapi_config=None)
    return {route.path for route in app.routes if route.path.startswith("/queues/events")}


def _sse_handler(router: Any, path: str) -> "HTTPRouteHandler":
    app = Litestar(route_handlers=[router], openapi_config=None)
    for route in app.routes:
        if isinstance(route, HTTPRoute) and route.path.endswith(path):
            for handler in route.route_handlers:
                if "GET" in handler.http_methods:
                    return handler
    msg = f"No SSE handler registered for {path!r}."
    raise AssertionError(msg)


async def _response_frames(response: Any) -> "list[bytes]":
    return [chunk async for chunk in response.iterator]


def _frame_data(frame: bytes) -> str:
    lines = frame.decode().splitlines()
    return "\n".join(line.removeprefix("data: ") for line in lines if line.startswith("data: "))


def _deny_guard(connection: ASGIConnection, route_handler: BaseRouteHandler) -> None:
    msg = "denied"
    raise PermissionDeniedException(detail=msg)

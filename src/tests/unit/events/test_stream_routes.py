import asyncio
from typing import TYPE_CHECKING, Any, cast

import pytest
from litestar import Litestar
from litestar.channels.backends.memory import MemoryChannelsBackend
from litestar.connection import ASGIConnection
from litestar.exceptions import WebSocketDisconnect, WebSocketException
from litestar.handlers.base import BaseRouteHandler
from litestar.testing import create_test_client

from litestar_queues.config import QueueConfig
from litestar_queues.events import EventConfig, EventStreamConfig, QueueChannels, QueueEvent
from litestar_queues.events.streaming import build_stream_router

if TYPE_CHECKING:
    from litestar.handlers.websocket_handlers import WebsocketRouteHandler


def test_build_stream_router_registers_all_scopes_by_default() -> None:
    router = build_stream_router(QueueConfig(), EventStreamConfig())

    assert _stream_paths(router) == {
        "/queues/events/tasks/{task_id:str}",
        "/queues/events/queues/{queue:str}",
        "/queues/events/workers/{worker_id:str}",
        "/queues/events/global",
        "/queues/events/custom/{scope_key:str}",
        "/queues/events/sse/tasks/{task_id:str}",
        "/queues/events/sse/queues/{queue:str}",
        "/queues/events/sse/workers/{worker_id:str}",
        "/queues/events/sse/global",
        "/queues/events/sse/custom/{scope_key:str}",
    }


def test_build_stream_router_narrows_to_configured_scopes() -> None:
    router = build_stream_router(QueueConfig(), EventStreamConfig(scopes={"task"}))

    assert _stream_paths(router) == {"/queues/events/tasks/{task_id:str}", "/queues/events/sse/tasks/{task_id:str}"}


def test_build_stream_router_ignores_unrecognized_scopes() -> None:
    stream_config = EventStreamConfig(scopes=cast("Any", {"task", "unknown"}))

    router = build_stream_router(QueueConfig(), stream_config)

    assert _stream_paths(router) == {"/queues/events/tasks/{task_id:str}", "/queues/events/sse/tasks/{task_id:str}"}


def test_stream_router_applies_guards_and_denies_before_accept() -> None:
    router = build_stream_router(QueueConfig(), EventStreamConfig(guards=[_deny_guard], scopes={"task"}))

    assert router.guards == [_deny_guard]
    with (
        create_test_client(route_handlers=[router], openapi_config=None) as client,
        pytest.raises(WebSocketDisconnect) as exc_info,
        client.websocket_connect("/queues/events/tasks/abc"),
    ):
        pass

    assert exc_info.value.code == 4001


@pytest.mark.anyio
async def test_task_stream_relays_from_channels_backend() -> None:
    channels = MemoryChannelsBackend(history=0)
    config = QueueConfig(event=EventConfig(channels_backend=channels))
    router = build_stream_router(config, EventStreamConfig(scopes={"task"}))
    handler = _stream_handler(router, "/tasks/{task_id:str}")
    socket = _RecordingSocket()
    event = QueueEvent(type="task.progress", scope="task", task_id="task-1", message="working")

    async def publish() -> None:
        await socket.accepted.wait()
        await asyncio.sleep(0.01)
        await channels.publish(event.to_json(), [QueueChannels.task("task-1")])

    await channels.on_startup()
    try:
        await asyncio.gather(handler.fn(socket, task_id="task-1"), publish())
    finally:
        await channels.on_shutdown()

    assert socket.accepted.is_set()
    assert socket.sent_json == [event.to_dict()]


def _stream_paths(router: Any) -> "set[str]":
    app = Litestar(route_handlers=[router], openapi_config=None)
    return {route.path for route in app.routes if route.path.startswith("/queues/events")}


def _stream_handler(router: Any, path: str) -> "WebsocketRouteHandler":
    for route in router.routes:
        handler = route.route_handler
        if path in handler.paths:
            return cast("WebsocketRouteHandler", handler)
    msg = f"No stream handler registered for {path!r}."
    raise AssertionError(msg)


def _deny_guard(connection: ASGIConnection, route_handler: BaseRouteHandler) -> None:
    raise WebSocketException(detail="denied", code=4001)


class _RecordingSocket:
    def __init__(self) -> None:
        self.accepted = asyncio.Event()
        self.sent_json: "list[dict[str, object]]" = []

    async def accept(self) -> None:
        self.accepted.set()

    async def send_json(self, data: "dict[str, object]") -> None:
        self.sent_json.append(data)
        msg = "stop after first event"
        raise RuntimeError(msg)

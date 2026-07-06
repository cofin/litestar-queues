from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, cast

import pytest
from litestar import Litestar
from litestar.connection import ASGIConnection
from litestar.exceptions import WebSocketDisconnect, WebSocketException
from litestar.handlers.base import BaseRouteHandler
from litestar.testing import create_test_client

from litestar_queues.config import QueueConfig
from litestar_queues.events import EventConfig, EventStreamConfig
from litestar_queues.events.streaming import build_stream_router

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Sequence

    from litestar.handlers.websocket_handlers import WebsocketRouteHandler

pytestmark = pytest.mark.anyio


class _EmptySubscriber:
    async def iter_events(self) -> "AsyncIterator[bytes]":
        if False:
            yield b""


class _FakeChannelsPlugin:
    def __init__(self) -> None:
        self.subscribed_channels: list[str] | None = None

    @asynccontextmanager
    async def start_subscription(
        self, channels: "Sequence[str]", history: int | None = None
    ) -> "AsyncIterator[_EmptySubscriber]":
        self.subscribed_channels = list(channels)
        yield _EmptySubscriber()


class _RecordingSocket:
    def __init__(self) -> None:
        self.accepted = False
        self.sent_json: list[dict[str, object]] = []

    async def accept(self) -> None:
        self.accepted = True

    async def send_json(self, data: dict[str, object]) -> None:
        self.sent_json.append(data)


async def test_channel_authorizer_receives_scope_and_key_before_accept() -> None:
    calls: list[tuple[Any, str, str | None, bool]] = []

    def authorize(connection: Any, scope: str, key: str | None) -> bool:
        calls.append((connection, scope, key, connection.accepted))
        return True

    channels = _FakeChannelsPlugin()
    socket = _RecordingSocket()
    router = build_stream_router(
        QueueConfig(event=EventConfig(channels_backend=channels)),
        EventStreamConfig(scopes={"task"}, channel_authorizer=authorize, heartbeat_interval=0),
    )

    await _stream_handler(router, "/tasks/{task_id:str}").fn(socket, task_id="task-9")

    assert calls == [(socket, "task", "task-9", False)]
    assert socket.accepted
    assert channels.subscribed_channels == ["litestar_queues:task:task-9:events"]


async def test_channel_authorizer_denies_with_4003_before_accept() -> None:
    channels = _FakeChannelsPlugin()
    socket = _RecordingSocket()
    router = build_stream_router(
        QueueConfig(event=EventConfig(channels_backend=channels)),
        EventStreamConfig(scopes={"task"}, channel_authorizer=lambda *_: False, heartbeat_interval=0),
    )

    with pytest.raises(WebSocketException) as exc_info:
        await _stream_handler(router, "/tasks/{task_id:str}").fn(socket, task_id="task-9")

    assert exc_info.value.code == 4003
    assert not socket.accepted
    assert channels.subscribed_channels is None
    assert socket.sent_json == []


async def test_async_channel_authorizer_supported() -> None:
    calls: list[tuple[str, str | None]] = []

    async def authorize(connection: Any, scope: str, key: str | None) -> bool:
        calls.append((scope, key))
        return True

    channels = _FakeChannelsPlugin()
    socket = _RecordingSocket()
    router = build_stream_router(
        QueueConfig(event=EventConfig(channels_backend=channels)),
        EventStreamConfig(scopes={"queue"}, channel_authorizer=authorize, heartbeat_interval=0),
    )

    await _stream_handler(router, "/queues/{queue:str}").fn(socket, queue="critical")

    assert calls == [("queue", "critical")]
    assert socket.accepted
    assert channels.subscribed_channels == ["litestar_queues:queue:critical:events"]


async def test_guard_auth_failure_closes_4001_before_authorizer() -> None:
    calls: list[tuple[str, str | None]] = []

    def authorize(connection: Any, scope: str, key: str | None) -> bool:
        calls.append((scope, key))
        return True

    router = build_stream_router(QueueConfig(), EventStreamConfig(guards=[_deny_guard], channel_authorizer=authorize))

    with (
        create_test_client(route_handlers=[router], openapi_config=None) as client,
        pytest.raises(WebSocketDisconnect) as exc_info,
        client.websocket_connect("/queues/events/tasks/task-9"),
    ):
        pass

    assert exc_info.value.code == 4001
    assert calls == []


def _stream_handler(router: Any, path: str) -> "WebsocketRouteHandler":
    app = Litestar(route_handlers=[router], openapi_config=None)
    for route in app.routes:
        if route.path.endswith(path):
            return cast("WebsocketRouteHandler", route.route_handler)
    msg = f"No stream handler registered for {path!r}."
    raise AssertionError(msg)


def _deny_guard(connection: ASGIConnection, route_handler: BaseRouteHandler) -> None:
    raise WebSocketException(detail="denied", code=4001)

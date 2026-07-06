"""Plugin-owned WebSocket streaming endpoints for queue events.

Imported only when a stream config is present and enabled, so base package
imports stay free of routing and Channels-driver imports.
"""

import asyncio
import contextlib
import time
from collections import OrderedDict
from collections.abc import Container, Sequence
from typing import TYPE_CHECKING, Any, Protocol

from litestar_queues.events.channels import QueueChannels
from litestar_queues.events.models import QueueEventScope

if TYPE_CHECKING:
    from litestar import Router, WebSocket

    from litestar_queues.config import QueueConfig
    from litestar_queues.events._typing import ChannelsLike
    from litestar_queues.events.stream_config import EventStreamConfig

__all__ = ("StreamMetrics", "build_stream_router", "stream_queue_events_hardened")


class StreamMetrics(Protocol):
    """Optional metric callbacks used by the hardened stream relay."""

    def on_connect(self, scope: "QueueEventScope") -> None:
        """Record a stream connection."""

    def on_event(self, scope: "QueueEventScope") -> None:
        """Record an event sent to a stream client."""

    def on_heartbeat(self, scope: "QueueEventScope") -> None:
        """Record a heartbeat sent to a stream client."""

    def on_dedup_drop(self, scope: "QueueEventScope") -> None:
        """Record a deduplicated event dropped by the stream relay."""

    def on_denial(self, scope: "QueueEventScope", reason: str) -> None:
        """Record an authorization denial."""

    def on_disconnect(self, scope: "QueueEventScope", duration_seconds: float) -> None:
        """Record stream connection lifetime."""


async def stream_queue_events_hardened(
    socket: Any,
    channels: Sequence[str],
    *,
    history: int = 0,
    channels_backend: "ChannelsLike | None" = None,
    heartbeat_interval: float = 25.0,
    stream_metrics: StreamMetrics | None = None,
    scope: QueueEventScope = "task",
) -> None:
    """Stream queue events to a WebSocket with a heartbeat and serialized sends.

    The caller owns route paths, guards, tenant filtering, and authorization.
    """
    from litestar_queues.events.litestar import _event_stream, _resolve_channels_backend

    backend = channels_backend or _resolve_channels_backend(socket)
    if backend is None:
        msg = "A Litestar Channels backend or plugin is required to stream queue events."
        raise RuntimeError(msg)

    await socket.accept()
    _record_metric(stream_metrics, "on_connect", scope)
    started_at = time.perf_counter()
    send_lock = asyncio.Lock()
    stop = asyncio.Event()
    try:
        async with _event_stream(backend, channels, history=history) as events:
            event_task = asyncio.create_task(_pump_events(socket, events, send_lock, stream_metrics, scope))
            heartbeat_task = asyncio.create_task(
                _pump_heartbeat(socket, send_lock, heartbeat_interval, stop, stream_metrics, scope)
            )
            await _wait_for_stream_tasks(event_task, heartbeat_task, stop)
    finally:
        _record_metric(stream_metrics, "on_disconnect", scope, time.perf_counter() - started_at)


async def _wait_for_stream_tasks(
    event_task: "asyncio.Task[None]", heartbeat_task: "asyncio.Task[None]", stop: "asyncio.Event"
) -> None:
    tasks = {event_task, heartbeat_task}
    try:
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            task.result()
        for task in pending:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
    finally:
        stop.set()
        for task in tasks:
            if not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task


async def _pump_events(
    socket: Any, events: "Any", send_lock: "asyncio.Lock", stream_metrics: StreamMetrics | None, scope: QueueEventScope
) -> None:
    from litestar_queues.events.litestar import _STREAM_DEDUP_MAX_KEYS, _decode_event

    seen_dedup_keys: "OrderedDict[str, None]" = OrderedDict()
    async for raw_event in events:
        event = _decode_event(raw_event)
        if event is None:
            continue
        dedup_key = event.event_key if event.event_key is not None else event.id
        if dedup_key in seen_dedup_keys:
            seen_dedup_keys.move_to_end(dedup_key)
            _record_metric(stream_metrics, "on_dedup_drop", scope)
            continue
        seen_dedup_keys[dedup_key] = None
        if len(seen_dedup_keys) > _STREAM_DEDUP_MAX_KEYS:
            seen_dedup_keys.popitem(last=False)
        if not await _send_json(socket, send_lock, event.to_dict()):
            return
        _record_metric(stream_metrics, "on_event", scope)


async def _pump_heartbeat(
    socket: Any,
    send_lock: "asyncio.Lock",
    interval: float,
    stop: "asyncio.Event",
    stream_metrics: StreamMetrics | None,
    scope: QueueEventScope,
) -> None:
    if interval <= 0:
        await stop.wait()
        return

    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass
        else:
            return
        if not await _send_json(socket, send_lock, {"type": "ping"}):
            return
        _record_metric(stream_metrics, "on_heartbeat", scope)


async def _send_json(socket: Any, send_lock: "asyncio.Lock", payload: dict[str, object]) -> bool:
    async with send_lock:
        try:
            await socket.send_json(payload)
        except (OSError, RuntimeError):
            return False
        except Exception as exc:
            if exc.__class__.__name__ == "WebSocketDisconnect":
                return False
            raise
    return True


def _record_metric(stream_metrics: StreamMetrics | None, method_name: str, *args: object) -> None:
    if stream_metrics is not None:
        getattr(stream_metrics, method_name)(*args)


def build_stream_router(config: "QueueConfig", stream_config: "EventStreamConfig") -> "Router":
    """Build plugin-owned queue-event WebSocket handlers for configured scopes.

    Returns:
        A router containing one WebSocket handler per recognized configured scope.
    """
    from litestar import Router

    from litestar_queues.events.litestar import _resolve_channels_backend

    history = stream_config.history

    async def _relay(socket: "WebSocket", scope: "QueueEventScope", channel: str) -> None:
        backend = _resolve_channels_backend(socket)
        if backend is None and config.event is not None:
            backend = config.event.channels_backend
        await stream_queue_events_hardened(
            socket,
            [channel],
            history=history,
            channels_backend=backend,
            heartbeat_interval=stream_config.heartbeat_interval,
            scope=scope,
        )

    handlers: list[Any] = []
    _append_task_handler(handlers, stream_config.scopes, _relay)
    _append_queue_handler(handlers, stream_config.scopes, _relay)
    _append_worker_handler(handlers, stream_config.scopes, _relay)
    _append_global_handler(handlers, stream_config.scopes, _relay)
    _append_custom_handler(handlers, stream_config.scopes, _relay)

    return Router(
        path=stream_config.path,
        route_handlers=handlers,
        guards=list(stream_config.guards) if stream_config.guards else None,
        opt=dict(stream_config.opt) if stream_config.opt else None,
        include_in_schema=stream_config.include_in_schema,
    )


def _append_task_handler(handlers: list[Any], scopes: Container[str], relay: Any) -> None:
    if "task" not in scopes:
        return

    from litestar import websocket
    from litestar.params import FromPath

    @websocket("/tasks/{task_id:str}", name="queue_event_stream_task")
    async def task_stream(socket: "WebSocket", task_id: FromPath[str]) -> None:
        await relay(socket, "task", QueueChannels.task(task_id))

    handlers.append(task_stream)


def _append_queue_handler(handlers: list[Any], scopes: Container[str], relay: Any) -> None:
    if "queue" not in scopes:
        return

    from litestar import websocket
    from litestar.params import FromPath

    @websocket("/queues/{queue:str}", name="queue_event_stream_queue")
    async def queue_stream(socket: "WebSocket", queue: FromPath[str]) -> None:
        await relay(socket, "queue", QueueChannels.queue(queue))

    handlers.append(queue_stream)


def _append_worker_handler(handlers: list[Any], scopes: Container[str], relay: Any) -> None:
    if "worker" not in scopes:
        return

    from litestar import websocket
    from litestar.params import FromPath

    @websocket("/workers/{worker_id:str}", name="queue_event_stream_worker")
    async def worker_stream(socket: "WebSocket", worker_id: FromPath[str]) -> None:
        await relay(socket, "worker", QueueChannels.worker(worker_id))

    handlers.append(worker_stream)


def _append_global_handler(handlers: list[Any], scopes: Container[str], relay: Any) -> None:
    if "global" not in scopes:
        return

    from litestar import websocket

    @websocket("/global", name="queue_event_stream_global")
    async def global_stream(socket: "WebSocket") -> None:
        await relay(socket, "global", QueueChannels.global_channel())

    handlers.append(global_stream)


def _append_custom_handler(handlers: list[Any], scopes: Container[str], relay: Any) -> None:
    if "custom" not in scopes:
        return

    from litestar import websocket
    from litestar.params import FromPath

    @websocket("/custom/{scope_key:str}", name="queue_event_stream_custom")
    async def custom_stream(socket: "WebSocket", scope_key: FromPath[str]) -> None:
        await relay(socket, "custom", QueueChannels.custom(scope_key))

    handlers.append(custom_stream)

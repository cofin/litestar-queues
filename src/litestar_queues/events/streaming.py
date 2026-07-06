"""Plugin-owned WebSocket streaming endpoints for queue events.

Imported only when a stream config is present and enabled, so base package
imports stay free of routing and Channels-driver imports.
"""

from collections.abc import Container
from typing import TYPE_CHECKING, Any

from litestar_queues.events.channels import QueueChannels

if TYPE_CHECKING:
    from litestar import Router, WebSocket

    from litestar_queues.config import QueueConfig
    from litestar_queues.events.stream_config import EventStreamConfig

__all__ = ("build_stream_router",)


def build_stream_router(config: "QueueConfig", stream_config: "EventStreamConfig") -> "Router":
    """Build plugin-owned queue-event WebSocket handlers for configured scopes.

    Returns:
        A router containing one WebSocket handler per recognized configured scope.
    """
    from litestar import Router

    from litestar_queues.events.litestar import _resolve_channels_backend, stream_queue_events

    history = stream_config.history

    async def _relay(socket: "WebSocket", channel: str) -> None:
        backend = _resolve_channels_backend(socket)
        if backend is None and config.event is not None:
            backend = config.event.channels_backend
        await stream_queue_events(socket, [channel], history=history, channels_backend=backend)

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
        await relay(socket, QueueChannels.task(task_id))

    handlers.append(task_stream)


def _append_queue_handler(handlers: list[Any], scopes: Container[str], relay: Any) -> None:
    if "queue" not in scopes:
        return

    from litestar import websocket
    from litestar.params import FromPath

    @websocket("/queues/{queue:str}", name="queue_event_stream_queue")
    async def queue_stream(socket: "WebSocket", queue: FromPath[str]) -> None:
        await relay(socket, QueueChannels.queue(queue))

    handlers.append(queue_stream)


def _append_worker_handler(handlers: list[Any], scopes: Container[str], relay: Any) -> None:
    if "worker" not in scopes:
        return

    from litestar import websocket
    from litestar.params import FromPath

    @websocket("/workers/{worker_id:str}", name="queue_event_stream_worker")
    async def worker_stream(socket: "WebSocket", worker_id: FromPath[str]) -> None:
        await relay(socket, QueueChannels.worker(worker_id))

    handlers.append(worker_stream)


def _append_global_handler(handlers: list[Any], scopes: Container[str], relay: Any) -> None:
    if "global" not in scopes:
        return

    from litestar import websocket

    @websocket("/global", name="queue_event_stream_global")
    async def global_stream(socket: "WebSocket") -> None:
        await relay(socket, QueueChannels.global_channel())

    handlers.append(global_stream)


def _append_custom_handler(handlers: list[Any], scopes: Container[str], relay: Any) -> None:
    if "custom" not in scopes:
        return

    from litestar import websocket
    from litestar.params import FromPath

    @websocket("/custom/{scope_key:str}", name="queue_event_stream_custom")
    async def custom_stream(socket: "WebSocket", scope_key: FromPath[str]) -> None:
        await relay(socket, QueueChannels.custom(scope_key))

    handlers.append(custom_stream)

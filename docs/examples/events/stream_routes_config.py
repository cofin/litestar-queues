"""Configuring SSE and WebSocket event stream endpoints with channel_authorizer and rollover."""

from typing import Any

from litestar import Litestar
from litestar.channels import ChannelsPlugin
from litestar.channels.backends.memory import MemoryChannelsBackend

from litestar_queues import QueueConfig, QueuePlugin, WorkerConfig
from litestar_queues.events import EventDeliveryConfig, EventStreamConfig, QueueEventsConfig, QueueEventScope


async def authorize_tenant_channel(connection: Any, scope: QueueEventScope, key: str | None) -> bool:
    """Authorize stream subscriptions based on request headers and requested scope."""
    tenant_header = connection.headers.get("x-tenant-id")
    if scope in {"task", "queue"}:
        return tenant_header is not None
    return scope == "custom" and key == f"tenant:{tenant_header}"


def build_streaming_app() -> Litestar:
    """Construct a Litestar application mounting SSE and WebSocket queue event stream routes."""
    channels_plugin = ChannelsPlugin(backend=MemoryChannelsBackend(), arbitrary_channels_allowed=True)
    queue_config = QueueConfig(
        queue_backend="memory",
        execution_backend="local",
        worker=WorkerConfig(placement="external"),
        events=QueueEventsConfig(
            delivery=EventDeliveryConfig(
                publish_task_channel=True, publish_queue_channel=True, publish_global_lifecycle=False
            ),
            stream=EventStreamConfig(
                path="/queues/events",
                transports={"sse", "websocket"},
                scopes={"task", "queue", "custom"},
                channel_authorizer=authorize_tenant_channel,
                heartbeat_interval=25.0,
                max_stream_duration=3000.0,
                retry_ms=3000,
            ),
        ),
    )
    return Litestar(plugins=[channels_plugin, QueuePlugin(config=queue_config)])


async def main() -> None:
    """Verify SSE and WebSocket stream routes are registered on the Litestar application."""
    app = build_streaming_app()
    route_paths = {route.path for route in app.routes}
    assert any("/queues/events" in path for path in route_paths)

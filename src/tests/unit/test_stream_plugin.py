import logging
import sys

import pytest
from litestar import Litestar
from litestar.channels import ChannelsPlugin
from litestar.channels.backends.memory import MemoryChannelsBackend

from litestar_queues import QueueConfig, QueuePlugin
from litestar_queues.events import EventConfig, EventStreamConfig
from litestar_queues.exceptions import QueueConfigurationError


def test_enabled_stream_config_registers_scope_routes() -> None:
    channels = _channels_arbitrary()
    plugin = QueuePlugin(QueueConfig(event=EventConfig(channels_backend=channels), event_stream=EventStreamConfig()))

    app = Litestar(plugins=[channels, plugin], openapi_config=None)

    assert _stream_paths(app) == {
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


def test_enabled_stream_config_respects_configured_scopes() -> None:
    channels = _channels_arbitrary()
    plugin = QueuePlugin(
        QueueConfig(
            event=EventConfig(channels_backend=channels),
            event_stream=EventStreamConfig(path="/events", scopes={"task"}),
        )
    )

    app = Litestar(plugins=[channels, plugin], openapi_config=None)

    assert _stream_paths(app) == {"/events/tasks/{task_id:str}", "/events/sse/tasks/{task_id:str}"}


def test_disabled_stream_config_registers_no_routes() -> None:
    sys.modules.pop("litestar_queues.events.streaming", None)
    default_app = Litestar(plugins=[QueuePlugin(QueueConfig())], openapi_config=None)
    disabled_app = Litestar(
        plugins=[QueuePlugin(QueueConfig(event_stream=EventStreamConfig(enabled=False)))], openapi_config=None
    )

    assert _stream_paths(default_app) == set()
    assert _stream_paths(disabled_app) == set()
    assert "litestar_queues.events.streaming" not in sys.modules


def test_enabled_without_auth_logs_single_warning(caplog: pytest.LogCaptureFixture) -> None:
    channels = _channels_arbitrary()
    plugin = QueuePlugin(QueueConfig(event=EventConfig(channels_backend=channels), event_stream=EventStreamConfig()))

    with caplog.at_level(logging.WARNING, logger="litestar_queues.plugin"):
        app = Litestar(plugins=[channels, plugin], openapi_config=None)

    records = [
        record for record in caplog.records if "Queue event streams have no configured authorization" in record.message
    ]
    assert len(records) == 1
    assert "scopes" not in records[0].message
    assert "docs/usage/event-streams.rst" in records[0].message
    assert _stream_paths(app)


def test_enabled_with_channel_authorizer_suppresses_auth_warning(caplog: pytest.LogCaptureFixture) -> None:
    channels = _channels_arbitrary()

    def allow_all(*_: object) -> bool:
        return True

    plugin = QueuePlugin(
        QueueConfig(
            event=EventConfig(channels_backend=channels), event_stream=EventStreamConfig(channel_authorizer=allow_all)
        )
    )

    with caplog.at_level(logging.WARNING, logger="litestar_queues.plugin"):
        Litestar(plugins=[channels, plugin], openapi_config=None)

    assert not caplog.records


def test_enabled_with_app_guard_suppresses_auth_warning(caplog: pytest.LogCaptureFixture) -> None:
    channels = _channels_arbitrary()

    def allow_all(*_: object) -> None:
        return None

    plugin = QueuePlugin(QueueConfig(event=EventConfig(channels_backend=channels), event_stream=EventStreamConfig()))

    with caplog.at_level(logging.WARNING, logger="litestar_queues.plugin"):
        Litestar(guards=[allow_all], plugins=[channels, plugin], openapi_config=None)

    assert not caplog.records


def test_explicit_unauthenticated_stream_suppresses_auth_warning(caplog: pytest.LogCaptureFixture) -> None:
    channels = _channels_arbitrary()
    plugin = QueuePlugin(
        QueueConfig(
            event=EventConfig(channels_backend=channels), event_stream=EventStreamConfig(allow_unauthenticated=True)
        )
    )

    with caplog.at_level(logging.WARNING, logger="litestar_queues.plugin"):
        Litestar(plugins=[channels, plugin], openapi_config=None)

    assert not caplog.records


def test_channels_plugin_without_arbitrary_channels_raises() -> None:
    channels = _channels_fixed()
    plugin = QueuePlugin(QueueConfig(event=EventConfig(channels_backend=channels), event_stream=EventStreamConfig()))

    with pytest.raises(QueueConfigurationError, match="arbitrary_channels_allowed=True"):
        Litestar(plugins=[channels, plugin], openapi_config=None)


def test_bare_channels_plugin_without_arbitrary_channels_raises() -> None:
    channels = _channels_fixed()
    plugin = QueuePlugin(QueueConfig(event_stream=EventStreamConfig()))

    with pytest.raises(QueueConfigurationError, match="arbitrary_channels_allowed=True"):
        Litestar(plugins=[channels, plugin], openapi_config=None)


def _stream_paths(app: Litestar) -> set[str]:
    return {route.path for route in app.routes if route.path.startswith(("/queues/events", "/events"))}


def _channels_arbitrary() -> ChannelsPlugin:
    return ChannelsPlugin(backend=MemoryChannelsBackend(history=0), arbitrary_channels_allowed=True)


def _channels_fixed() -> ChannelsPlugin:
    return ChannelsPlugin(backend=MemoryChannelsBackend(history=0), channels=["litestar_queues:global:events"])

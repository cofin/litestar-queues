from dataclasses import fields
from typing import get_type_hints

import pytest


def test_event_stream_config_defaults() -> "None":
    from litestar_queues.events import EventStreamConfig

    config = EventStreamConfig()

    assert config.enabled is True
    assert config.sse is True
    assert config.path == "/queues/events"
    assert config.guards is None
    assert config.channel_authorizer is None
    assert config.scopes == {"task", "queue", "worker", "global", "custom"}
    assert config.heartbeat_interval == 25.0
    assert config.history == 0
    assert config.include_in_schema is False
    assert config.opt is None


def test_event_stream_config_type_hints_resolve() -> "None":
    from litestar_queues.events.stream_config import ChannelAuthorizer, EventStreamConfig

    hints = get_type_hints(EventStreamConfig)

    assert hints["channel_authorizer"] == ChannelAuthorizer | None


def test_queue_config_carries_stream_config() -> "None":
    from litestar_queues.config import QueueConfig
    from litestar_queues.events import EventStreamConfig

    config = QueueConfig()

    assert config.event_stream is None
    assert config.signature_namespace["EventStreamConfig"] is EventStreamConfig
    assert config.queue_event_stream_state_key == "queue_event_stream"


def test_queue_config_sub_config_field_defaults() -> "None":
    from litestar_queues.config import QueueConfig
    from litestar_queues.events import EventConfig, EventLogConfig, EventStreamConfig
    from litestar_queues.observability import ObservabilityConfig

    config = QueueConfig()
    field_names = {field.name for field in fields(QueueConfig)}

    assert config.event is None
    assert config.event_stream is None
    assert config.event_log is None
    assert config.observability is None
    assert EventConfig().enabled is True
    assert EventLogConfig().enabled is True
    assert EventLogConfig().max_records == 1000
    assert EventStreamConfig().enabled is True
    assert config.signature_namespace["EventConfig"] is EventConfig
    assert config.signature_namespace["EventLogConfig"] is EventLogConfig
    assert config.signature_namespace["ObservabilityConfig"] is ObservabilityConfig
    assert {"event", "event_stream", "event_log", "observability"}.issubset(field_names)
    assert {"event_config", "event_log_config", "observability_config"}.isdisjoint(field_names)


def test_queue_config_explicit_disable_keeps_config_object() -> "None":
    from litestar_queues.config import QueueConfig
    from litestar_queues.events import EventConfig, EventLogConfig, EventStreamConfig

    config = QueueConfig(
        event=EventConfig(enabled=False),
        event_log=EventLogConfig(enabled=False),
        event_stream=EventStreamConfig(enabled=False),
    )

    assert config.event is not None
    assert config.event.enabled is False
    assert config.event_log is not None
    assert config.event_log.enabled is False
    assert config.event_stream is not None
    assert config.event_stream.enabled is False


def test_event_log_config_requires_positive_max_records() -> "None":
    from litestar_queues.events import EventLogConfig

    with pytest.raises(ValueError, match="max_records"):
        EventLogConfig(max_records=0)

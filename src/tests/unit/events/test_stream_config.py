from dataclasses import fields
from typing import get_type_hints

import pytest

from litestar_queues.exceptions import QueueConfigurationError


def test_event_stream_config_defaults() -> "None":
    from litestar_queues.events import EventStreamConfig

    config = EventStreamConfig()

    assert config.transports == {"sse", "websocket"}
    assert config.path == "/queues/events"
    assert config.guards is None
    assert config.channel_authorizer is None
    assert config.unauthenticated_access == "warn"
    assert config.scopes == {"task", "queue", "worker", "global", "custom"}
    assert config.heartbeat_interval == 25.0
    assert config.replay_limit == 0
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

    assert config.events is None
    assert config.signature_namespace["EventStreamConfig"] is EventStreamConfig


def test_queue_config_sub_config_field_defaults() -> "None":
    from litestar_queues.config import QueueConfig
    from litestar_queues.events import EventDeliveryConfig, EventHistoryConfig
    from litestar_queues.observability import ObservabilityConfig

    config = QueueConfig()
    field_names = {field.name for field in fields(QueueConfig)}

    assert config.events is None
    assert config.observability is None
    assert EventHistoryConfig().memory_capacity == 1000
    assert config.signature_namespace["EventDeliveryConfig"] is EventDeliveryConfig
    assert config.signature_namespace["EventHistoryConfig"] is EventHistoryConfig
    assert config.signature_namespace["ObservabilityConfig"] is ObservabilityConfig
    assert {"events", "observability"}.issubset(field_names)
    assert {"event", "event_stream", "event_log"}.isdisjoint(field_names)


def test_queue_config_disables_all_event_capabilities_by_absence() -> "None":
    from litestar_queues.config import QueueConfig

    assert QueueConfig(events=None).events is None


def test_event_log_config_requires_positive_memory_capacity() -> "None":
    from litestar_queues.events import EventHistoryConfig

    with pytest.raises(QueueConfigurationError, match="memory_capacity"):
        EventHistoryConfig(memory_capacity=0)

from litestar_queues import QueueConfig


def test_buffer_defaults() -> None:
    from litestar_queues.events import EventBufferConfig

    config = EventBufferConfig()

    assert config.enabled is True
    assert config.buffer_size == 20
    assert config.flush_interval == 0.5
    assert config.max_pending == 2000
    assert config.overflow == "drop_oldest"


def test_event_config_buffer_default_factory() -> None:
    from litestar_queues.events import EventBufferConfig, EventConfig

    first = EventConfig()
    second = EventConfig()

    assert isinstance(first.buffer, EventBufferConfig)
    assert first.buffer.enabled is True
    assert first.buffer is not second.buffer


def test_signature_namespace_has_buffer_config() -> None:
    from litestar_queues.events import EventBufferConfig

    namespace = QueueConfig().signature_namespace

    assert namespace["EventBufferConfig"] is EventBufferConfig

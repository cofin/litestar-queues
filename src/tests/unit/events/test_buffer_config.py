def test_buffer_defaults() -> None:
    from litestar_queues.events import EventBufferConfig

    config = EventBufferConfig()

    assert config.batch_size == 20
    assert config.flush_interval == 0.5
    assert config.max_pending == 2000
    assert config.overflow == "drop_oldest"


def test_event_config_buffer_default_factory() -> None:
    from litestar_queues.events import EventBufferConfig, EventDeliveryConfig

    first = EventDeliveryConfig()
    second = EventDeliveryConfig()

    assert isinstance(first.buffer, EventBufferConfig)
    assert first.buffer is not second.buffer


def test_event_buffer_config_is_publicly_exported() -> None:
    import litestar_queues
    from litestar_queues.events import EventBufferConfig

    assert litestar_queues.EventBufferConfig is EventBufferConfig
    assert "EventBufferConfig" in litestar_queues.__all__

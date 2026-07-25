from litestar_queues import QueueConfig, WorkerConfig


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


def test_signature_namespace_has_buffer_config() -> None:
    from litestar_queues.events import EventBufferConfig

    namespace = QueueConfig(worker=WorkerConfig(placement="external"), queue_backend="memory").signature_namespace

    assert namespace["EventBufferConfig"] is EventBufferConfig

import pytest

from litestar_queues.exceptions import QueueConfigurationError
from litestar_queues.execution.rabbitmq import RabbitMQExecutionConfig


@pytest.mark.parametrize(
    "overrides",
    [
        {"amqp_url": ""},
        {"amqp_url": "http://localhost"},
        {"amqp_url": "amqp://localhost", "queue_name": ""},
        {"amqp_url": "amqp://localhost", "dispatch_stale_after": 0},
        {"amqp_url": "amqp://localhost", "api_timeout": 0},
        {"amqp_url": "amqp://localhost", "delayed_retry_min_ms": 0},
        {"amqp_url": "amqp://localhost", "delayed_retry_min_ms": 2, "delayed_retry_max_ms": 1},
        {"amqp_url": "amqp://localhost", "consumer_timeout_ms": 0},
    ],
)
def test_rabbitmq_config_rejects_invalid_values(overrides: "dict[str, object]") -> "None":
    with pytest.raises(QueueConfigurationError):
        RabbitMQExecutionConfig(**overrides)  # type: ignore[arg-type]


def test_rabbitmq_config_defaults() -> "None":
    config = RabbitMQExecutionConfig(amqp_url="amqps://rabbit.example/vhost")

    assert config.queue_name is None
    assert config.declare_queue is True
    assert config.delayed_retry_type == "returned"
    assert config.delayed_retry_min_ms == 1_000
    assert config.delayed_retry_max_ms == 30_000
    assert config.consumer_timeout_ms is None
    assert "rabbit.example" not in repr(config)

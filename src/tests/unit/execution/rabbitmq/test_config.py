import pytest

from litestar_queues.exceptions import QueueConfigurationError
from litestar_queues.execution.rabbitmq import RabbitMQExecutionConfig


@pytest.fixture(autouse=True)
def supported_python(monkeypatch: "pytest.MonkeyPatch") -> "None":
    monkeypatch.setattr("litestar_queues.execution.rabbitmq.config._PYTHON_VERSION", (3, 11))


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


def test_rabbitmq_config_rejects_python_310(monkeypatch: "pytest.MonkeyPatch") -> "None":
    monkeypatch.setattr("litestar_queues.execution.rabbitmq.config._PYTHON_VERSION", (3, 10))

    with pytest.raises(QueueConfigurationError, match=r"Python 3\.11"):
        RabbitMQExecutionConfig(amqp_url="amqp://localhost")

import pytest

from litestar_queues.exceptions import QueueConfigurationError
from litestar_queues.execution.kafka import KafkaExecutionConfig


@pytest.mark.parametrize(
    "overrides",
    [
        {"bootstrap_servers": ""},
        {"bootstrap_servers": "localhost:9092", "topic": ""},
        {"bootstrap_servers": "localhost:9092", "consumer_group": ""},
        {"bootstrap_servers": "localhost:9092", "dispatch_stale_after": 0},
        {"bootstrap_servers": "localhost:9092", "api_timeout": 0},
    ],
)
def test_kafka_config_rejects_invalid_values(overrides: "dict[str, object]") -> "None":
    with pytest.raises(QueueConfigurationError):
        KafkaExecutionConfig(**overrides)  # type: ignore[arg-type]


def test_kafka_config_defaults() -> "None":
    config = KafkaExecutionConfig(bootstrap_servers="kafka.example:9092")

    assert config.topic == "litestar-queues"
    assert config.consumer_group == "litestar-queues"
    assert config.dispatch_stale_after == 60
    assert "kafka.example" not in repr(config)


def test_kafka_config_rejects_options_owned_by_backend() -> "None":
    with pytest.raises(QueueConfigurationError, match=r"producer_options\.bootstrap_servers"):
        KafkaExecutionConfig(
            bootstrap_servers="localhost:9092", producer_options={"bootstrap_servers": "elsewhere:9092"}
        )

    with pytest.raises(QueueConfigurationError, match=r"consumer_options\.group_id"):
        KafkaExecutionConfig(bootstrap_servers="localhost:9092", consumer_options={"group_id": "other"})

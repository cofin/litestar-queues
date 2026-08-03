import pytest

from litestar_queues.exceptions import QueueConfigurationError
from litestar_queues.execution.sqs import SqsExecutionConfig


@pytest.mark.parametrize(
    "overrides",
    [
        {"queue_url": ""},
        {"queue_url": "url", "wait_time_seconds": 21},
        {"queue_url": "url", "receive_batch_size": 0},
        {"queue_url": "url", "visibility_timeout": 43_201},
        {"queue_url": "url", "visibility_timeout": 30, "visibility_extension_interval": 30},
        {"queue_url": "url", "dispatch_stale_after": 0},
        {"queue_url": "url", "api_timeout": 0},
        {"queue_url": "url", "message_group_id": "group"},
    ],
)
def test_sqs_config_rejects_invalid_aws_and_cross_field_values(overrides: "dict[str, object]") -> "None":
    with pytest.raises(QueueConfigurationError):
        SqsExecutionConfig(**overrides)  # type: ignore[arg-type]


def test_sqs_config_defaults_match_long_poll_consumer_contract() -> "None":
    config = SqsExecutionConfig(queue_url="https://sqs.us-east-1.amazonaws.com/123/queue")

    assert config.wait_time_seconds == 20
    assert config.receive_batch_size == 10
    assert config.visibility_timeout == 60
    assert config.visibility_extension_interval == 30
    assert config.dispatch_stale_after == 60
    assert config.api_timeout == 30

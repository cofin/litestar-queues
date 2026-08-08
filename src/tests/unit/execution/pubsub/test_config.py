import pytest

from litestar_queues import QueueConfig
from litestar_queues.exceptions import QueueConfigurationError
from litestar_queues.execution.pubsub import PubSubExecutionConfig


@pytest.mark.parametrize(
    "overrides",
    [
        {"project_id": "", "topic_id": "tasks", "subscription_id": "workers"},
        {"project_id": "project", "topic_id": "", "subscription_id": "workers"},
        {"project_id": "project", "topic_id": "tasks", "subscription_id": ""},
        {"project_id": "project", "topic_id": "tasks", "subscription_id": "workers", "ack_deadline": 9},
        {
            "project_id": "project",
            "topic_id": "tasks",
            "subscription_id": "workers",
            "ack_deadline": 30,
            "ack_extension_interval": 30,
        },
        {"project_id": "project", "topic_id": "tasks", "subscription_id": "workers", "dispatch_stale_after": 0},
        {"project_id": "project", "topic_id": "tasks", "subscription_id": "workers", "api_timeout": 0},
    ],
)
def test_pubsub_config_rejects_invalid_values(overrides: "dict[str, object]") -> "None":
    with pytest.raises(QueueConfigurationError):
        PubSubExecutionConfig(**overrides)  # type: ignore[arg-type]


def test_pubsub_config_builds_resource_paths() -> "None":
    config = PubSubExecutionConfig(project_id="project", topic_id="tasks", subscription_id="workers")

    assert config.topic_path == "projects/project/topics/tasks"
    assert config.subscription_path == "projects/project/subscriptions/workers"
    assert config.ack_deadline == 60
    assert config.ack_extension_interval == 30
    assert config.dispatch_stale_after == 60


def test_pubsub_config_is_a_typed_execution_selector() -> "None":
    execution_config = PubSubExecutionConfig(project_id="project", topic_id="tasks", subscription_id="workers")

    assert QueueConfig(execution_backend=execution_config).execution_backend is execution_config
    assert execution_config.backend_name == "pubsub"

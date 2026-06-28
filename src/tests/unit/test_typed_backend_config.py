from dataclasses import dataclass
from typing import ClassVar

from litestar_queues import QueueConfig
from litestar_queues.backends import BaseQueueBackend, queue_backend
from litestar_queues.execution import BaseExecutionBackend, execution_backend


@dataclass(frozen=True, slots=True)
class CustomQueueBackendConfig:
    backend_name: "ClassVar[str]" = "typed-test-queue"
    value: "str" = "queue-configured"


@dataclass(frozen=True, slots=True)
class CustomExecutionBackendConfig:
    backend_name: "ClassVar[str]" = "typed-test-execution"
    value: "str" = "execution-configured"


@queue_backend(CustomQueueBackendConfig.backend_name)
class TypedQueueBackend(BaseQueueBackend):
    __slots__ = ("backend_config",)

    def __init__(
        self, config: "QueueConfig | None" = None, *, backend_config: "CustomQueueBackendConfig | None" = None
    ) -> "None":
        super().__init__(config=config)
        self.backend_config = backend_config


@execution_backend(CustomExecutionBackendConfig.backend_name)
class TypedExecutionBackend(BaseExecutionBackend):
    __slots__ = ("execution_config",)

    def __init__(
        self, config: "QueueConfig | None" = None, *, execution_config: "CustomExecutionBackendConfig | None" = None
    ) -> "None":
        super().__init__(config=config)
        self.execution_config = execution_config


def test_queue_config_selects_typed_queue_backend() -> "None":
    backend_config = CustomQueueBackendConfig(value="configured")
    config = QueueConfig(queue_backend=backend_config)

    backend = config.get_queue_backend()

    assert isinstance(backend, TypedQueueBackend)
    assert backend.config is config
    assert backend.backend_config is backend_config


def test_queue_config_selects_typed_execution_backend() -> "None":
    execution_config = CustomExecutionBackendConfig(value="configured")
    config = QueueConfig(execution_backend=execution_config)

    backend = config.get_execution_backend()

    assert isinstance(backend, TypedExecutionBackend)
    assert backend.config is config
    assert backend.execution_config is execution_config


def test_cloudrun_execution_config_selects_cloudrun_backend() -> "None":
    from litestar_queues.execution.cloudrun import CloudRunExecutionBackend, CloudRunExecutionConfig

    execution_config = CloudRunExecutionConfig(project_id="test-project", job_name="worker")
    config = QueueConfig(execution_backend=execution_config)

    backend = config.get_execution_backend()

    assert isinstance(backend, CloudRunExecutionBackend)
    assert backend.execution_config is execution_config

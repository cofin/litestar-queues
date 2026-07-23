from dataclasses import dataclass
from typing import ClassVar

import pytest

from litestar_queues import QueueConfig, WorkerConfig
from litestar_queues.backends import BaseQueueBackend, queue_backend
from litestar_queues.exceptions import QueueConfigurationError
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


def test_queue_config_poll_backoff_is_enabled_by_default() -> "None":
    """Adaptive polling backoff is the default; a bare QueueConfig() opts in automatically."""
    config = QueueConfig()

    assert config.worker.poll_backoff_max == 30.0
    assert config.worker.poll_backoff_multiplier == 2.0
    assert config.worker.poll_jitter == 0.15


def test_queue_config_poll_backoff_max_none_opts_out_to_fixed_polling() -> "None":
    """worker_poll_backoff_max=None is the explicit, still-supported opt-out to fixed polling."""
    config = QueueConfig(worker=WorkerConfig(poll_backoff_max=None))

    assert config.worker.poll_backoff_max is None


def test_queue_config_poll_backoff_accepts_boundary_values() -> "None":
    """Multiplier of one, maximum equal to base, and jitter zero/one are valid boundaries."""
    QueueConfig(worker=WorkerConfig(poll_interval=1.0, poll_backoff_max=1.0, poll_backoff_multiplier=1.0))
    QueueConfig(worker=WorkerConfig(poll_interval=1.0, poll_backoff_max=2.0, poll_jitter=0.0))
    QueueConfig(worker=WorkerConfig(poll_interval=1.0, poll_backoff_max=2.0, poll_jitter=1.0))


def test_queue_config_rejects_backoff_max_below_base_interval() -> "None":
    with pytest.raises(QueueConfigurationError, match=r"WorkerConfig\.poll_backoff_max"):
        QueueConfig(worker=WorkerConfig(poll_interval=1.0, poll_backoff_max=0.5))


@pytest.mark.parametrize("invalid_max", [0.0, -1.0])
def test_queue_config_rejects_non_positive_backoff_max(invalid_max: "float") -> "None":
    with pytest.raises(QueueConfigurationError, match=r"WorkerConfig\.poll_backoff_max"):
        QueueConfig(worker=WorkerConfig(poll_backoff_max=invalid_max))


def test_queue_config_rejects_multiplier_below_one() -> "None":
    with pytest.raises(QueueConfigurationError, match=r"WorkerConfig\.poll_backoff_multiplier"):
        QueueConfig(worker=WorkerConfig(poll_backoff_max=1.0, poll_backoff_multiplier=0.99))


def test_queue_config_rejects_multiplier_below_one_even_without_backoff_max() -> "None":
    with pytest.raises(QueueConfigurationError, match=r"WorkerConfig\.poll_backoff_multiplier"):
        QueueConfig(worker=WorkerConfig(poll_backoff_multiplier=0.5))


@pytest.mark.parametrize("invalid_jitter", [-0.01, 1.01])
def test_queue_config_rejects_jitter_outside_unit_interval(invalid_jitter: "float") -> "None":
    with pytest.raises(QueueConfigurationError, match=r"WorkerConfig\.poll_jitter"):
        QueueConfig(worker=WorkerConfig(poll_backoff_max=1.0, poll_jitter=invalid_jitter))


def test_queue_config_rejects_jitter_outside_unit_interval_even_without_backoff_max() -> "None":
    with pytest.raises(QueueConfigurationError, match=r"WorkerConfig\.poll_jitter"):
        QueueConfig(worker=WorkerConfig(poll_jitter=1.5))

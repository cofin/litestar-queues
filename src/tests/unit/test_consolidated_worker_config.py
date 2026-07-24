import dataclasses

import pytest

from litestar_queues import QueueConfig, TaskRequest, WorkerConfig
from litestar_queues.backends.memory import InMemoryQueueBackend
from litestar_queues.exceptions import QueueConfigurationError
from litestar_queues.service import QueueService
from litestar_queues.worker import Worker


def test_worker_receives_one_config_object() -> None:
    worker_config = WorkerConfig(id="worker-a", run_in_app=False, max_concurrency=4, queues=("reports",))
    queue_config = QueueConfig(worker=worker_config)
    service = QueueService(queue_config, queue_backend=InMemoryQueueBackend(queue_config))

    worker = Worker(service, worker_config)

    assert worker.worker_id == "worker-a"
    assert worker._max_concurrency == 4
    assert worker._queues == ("reports",)


def test_worker_cli_overrides_can_copy_without_mutating_app_config() -> None:
    configured = WorkerConfig(max_concurrency=2)
    overridden = dataclasses.replace(configured, max_concurrency=8)

    assert configured.max_concurrency == 2
    assert overridden.max_concurrency == 8


def test_worker_startup_timeout_must_be_positive() -> None:
    assert WorkerConfig(startup_timeout=0.25).startup_timeout == 0.25

    with pytest.raises(QueueConfigurationError, match=r"WorkerConfig\.startup_timeout"):
        WorkerConfig(startup_timeout=0)


def test_task_request_names_the_bulk_enqueue_input() -> None:
    request = TaskRequest(task_name="reports.generate", args=("report-1",))

    assert request.task_name == "reports.generate"
    assert request.args == ("report-1",)


def test_sync_thread_pool_size_defaults_to_the_anyio_thread_limiter() -> None:
    config = QueueConfig()

    assert config.sync_thread_pool_size == 40
    assert config.sync_thread_name_prefix == "litestar-queues"


@pytest.mark.parametrize("size", [0, -1])
def test_sync_thread_pool_size_must_be_positive(size: int) -> None:
    with pytest.raises(QueueConfigurationError, match="sync_thread_pool_size must be greater than 0"):
        QueueConfig(sync_thread_pool_size=size)

import dataclasses
import os
from pathlib import Path

import pytest

import litestar_queues.config as config_module
from litestar_queues import QueueConfig, TaskRequest, WorkerConfig
from litestar_queues.backends.memory import InMemoryQueueBackend
from litestar_queues.exceptions import QueueConfigurationError
from litestar_queues.service import QueueService
from litestar_queues.worker import Worker


def test_worker_receives_one_config_object() -> None:
    worker_config = WorkerConfig(id="worker-a", placement="external", max_concurrency=4, queues=("reports",))
    queue_config = QueueConfig(queue_backend="memory", worker=worker_config)
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


@pytest.mark.parametrize("fraction", [0.0, 1.0])
def test_worker_heartbeat_jitter_accepts_inclusive_boundaries(fraction: float) -> None:
    worker_config = WorkerConfig(heartbeat_jitter_fraction=fraction)

    assert worker_config.heartbeat_jitter_fraction == fraction


@pytest.mark.parametrize("fraction", [-0.1, 1.1])
def test_worker_heartbeat_jitter_rejects_out_of_range_values(fraction: float) -> None:
    with pytest.raises(QueueConfigurationError, match=r"WorkerConfig\.heartbeat_jitter_fraction"):
        WorkerConfig(heartbeat_jitter_fraction=fraction)


def test_worker_queue_concurrency_validates_names_caps_and_selected_queues() -> None:
    assert WorkerConfig(queue_concurrency={"email": 1}).queue_concurrency == {"email": 1}

    with pytest.raises(QueueConfigurationError, match="queue names"):
        WorkerConfig(queue_concurrency={"": 1})
    with pytest.raises(QueueConfigurationError, match="values"):
        WorkerConfig(queue_concurrency={"email": 0})
    with pytest.raises(QueueConfigurationError, match=r"not in WorkerConfig\.queues"):
        WorkerConfig(queues=("reports",), queue_concurrency={"email": 1})


def test_task_request_names_the_bulk_enqueue_input() -> None:
    request = TaskRequest(task_name="reports.generate", args=("report-1",))

    assert request.task_name == "reports.generate"
    assert request.args == ("report-1",)


def test_sync_thread_pool_size_defaults_to_cgroup_aware_executor_policy(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config_module, "_effective_cpu_count", lambda: 2)

    config = QueueConfig(worker=WorkerConfig(placement="external"), queue_backend="memory")

    assert config.sync_thread_pool_size == 6
    assert config.sync_thread_name_prefix == "litestar-queues"


def test_explicit_sync_thread_pool_size_is_preserved(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config_module, "_effective_cpu_count", lambda: 2)

    config = QueueConfig(worker=WorkerConfig(placement="external"), queue_backend="memory", sync_thread_pool_size=40)

    assert config.sync_thread_pool_size == 40


def test_cgroup_v2_cpu_quota_is_rounded_up(tmp_path: Path) -> None:
    cpu_max = tmp_path / "cpu.max"
    cpu_max.write_text("150000 100000\n")

    assert config_module._cgroup_cpu_limit(cpu_max=cpu_max) == 2  # pyright: ignore[reportPrivateUsage]


def test_effective_cpu_count_honors_cgroup_quota(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config_module, "_cgroup_cpu_limit", lambda: 2)
    monkeypatch.setattr(os, "cpu_count", lambda: 16)
    monkeypatch.setattr(os, "sched_getaffinity", lambda _pid: set(range(8)), raising=False)
    monkeypatch.setattr(os, "process_cpu_count", lambda: 8, raising=False)

    assert config_module._effective_cpu_count() == 2  # pyright: ignore[reportPrivateUsage]


@pytest.mark.parametrize("size", [0, -1])
def test_sync_thread_pool_size_must_be_positive(size: int) -> None:
    with pytest.raises(QueueConfigurationError, match="sync_thread_pool_size must be greater than 0"):
        QueueConfig(worker=WorkerConfig(placement="external"), queue_backend="memory", sync_thread_pool_size=size)

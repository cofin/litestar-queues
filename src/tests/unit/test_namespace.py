from dataclasses import replace
from pathlib import Path

import pytest

from litestar_queues import QueueChannels, QueueConfig, QueueNamespace, WorkerConfig
from litestar_queues.exceptions import QueueConfigurationError


def test_queue_config_exposes_format_specific_namespace_names() -> None:
    config = QueueConfig(namespace="myapp")

    assert config.namespace == "myapp"
    assert config.names == QueueNamespace("myapp")
    assert config.names.metric("wakeups") == "myapp.wakeups"
    assert config.names.logger("worker") == "myapp.worker"
    assert config.names.channel("worker_wakeups") == "myapp:worker_wakeups"
    assert config.names.key("tasks") == "myapp:tasks"
    assert config.names.registration("service") == "myapp_service"
    assert config.names.environment("task_id") == "MYAPP_TASK_ID"
    assert config.names.resource("worker") == "myapp-worker"
    assert config.names.coordination("maintenance") == "myapp-maintenance"
    assert config.service_state_key == "myapp_service"
    assert config.worker_state_key == "myapp_worker"
    assert config.event_publisher_state_key == "myapp_event_publisher"
    assert config.event_channels_state_key == "myapp_event_channels"
    assert config.observability_runtime_state_key == "myapp_observability_runtime"
    assert config.maintenance_name == "myapp-maintenance"


def test_default_namespace_preserves_legacy_name_families() -> None:
    config = QueueConfig()

    assert config.namespace == "litestar_queues"
    assert config.names.metric("wakeups") == "litestar_queues.wakeups"
    assert config.names.logger("worker") == "litestar_queues.worker"
    assert config.names.channel("worker_wakeups") == "litestar_queues:worker_wakeups"
    assert config.names.key("tasks") == "litestar_queues:tasks"
    assert config.names.registration("service") == "queue_service"
    assert config.names.environment("task_id") == "LITESTAR_QUEUES_TASK_ID"
    assert config.names.resource("worker") == "litestar-queues-worker"
    assert config.names.coordination("maintenance") == "queue-maintenance"
    assert config.service_dependency_key == "queue_service"
    assert config.events_dependency_key == "queue_events"
    assert config.sync_thread_name_prefix == "litestar-queues"
    assert config.scheduler_canary_task == "scheduler.heartbeat"
    assert config.event_publisher_state_key == "queue_event_publisher"
    assert config.observability_runtime_state_key == "queue_observability_runtime"


def test_custom_namespace_derives_package_owned_config_defaults() -> None:
    config = QueueConfig(namespace="myapp")

    assert config.service_dependency_key == "myapp_service"
    assert config.events_dependency_key == "myapp_events"
    assert config.sync_thread_name_prefix == "myapp"
    assert config.scheduler_canary_task == "myapp.scheduler.heartbeat"


def test_explicit_package_owned_names_override_namespace_defaults() -> None:
    config = QueueConfig(
        namespace="myapp",
        service_dependency_key="service",
        events_dependency_key="events",
        sync_thread_name_prefix="threads",
        scheduler_canary_task="ops.healthcheck",
    )

    assert config.service_dependency_key == "service"
    assert config.events_dependency_key == "events"
    assert config.sync_thread_name_prefix == "threads"
    assert config.scheduler_canary_task == "ops.healthcheck"


@pytest.mark.parametrize(
    "namespace",
    ["", "MYAPP", " myapp", "myapp ", "_myapp", "myapp_", "myapp__queues", "myapp.queues", "myapp-queues", "1myapp"],
)
def test_namespace_rejects_ambiguous_roots(namespace: str) -> None:
    with pytest.raises(QueueConfigurationError, match=r"QueueConfig\.namespace"):
        QueueConfig(namespace=namespace)


def test_namespace_does_not_change_user_task_or_queue_names() -> None:
    worker = WorkerConfig(placement="external", queues=("critical", "reports"))
    config = QueueConfig(
        namespace="myapp", queue_backend="memory", worker=worker, scheduler_canary_task="app.scheduler.healthcheck"
    )

    assert config.worker.queues == ("critical", "reports")
    assert config.scheduler_canary_task == "app.scheduler.healthcheck"
    assert replace(config, namespace="analytics").worker.queues == ("critical", "reports")


def test_namespace_does_not_change_sql_table_configuration() -> None:
    pytest.importorskip("sqlspec")
    from litestar_queues.backends.sqlspec import SQLSpecBackendConfig

    backend = SQLSpecBackendConfig(
        queue_table_name="jobs",
        event_history_table_name="job_events",
        maintenance_table_name="job_maintenance",
        task_reservation_table_name="job_reservations",
    )
    config = QueueConfig(namespace="myapp", queue_backend=backend, worker=WorkerConfig(placement="external"))

    assert config.queue_backend is backend
    assert backend.queue_table_name == "jobs"
    assert backend.event_history_table_name == "job_events"
    assert backend.maintenance_table_name == "job_maintenance"
    assert backend.task_reservation_table_name == "job_reservations"


def test_queue_channels_accept_namespace_without_mutating_legacy_default() -> None:
    assert QueueChannels.task("task-1", namespace="myapp") == "myapp:task:task-1:events"
    assert QueueChannels.queue("reports", namespace=QueueNamespace("myapp")) == "myapp:queue:reports:events"
    assert QueueChannels.task("task-1") == "litestar_queues:task:task-1:events"


def test_redis_runtime_keys_derive_from_namespace_and_explicit_values_win() -> None:
    pytest.importorskip("redis")
    from litestar_queues.backends.redis import RedisBackendConfig, RedisQueueBackend

    derived = RedisQueueBackend(
        QueueConfig(namespace="myapp", queue_backend=RedisBackendConfig(), worker=WorkerConfig(placement="external")),
        backend_config=RedisBackendConfig(),
    )
    explicit = RedisQueueBackend(
        QueueConfig(namespace="myapp", worker=WorkerConfig(placement="external")),
        backend_config=RedisBackendConfig(key_prefix="jobs", wakeup_channel="jobs:wakeups"),
    )

    assert derived._key_prefix == "myapp"
    assert derived._wakeup_channel == "myapp:worker_wakeups"
    assert explicit._key_prefix == "jobs"
    assert explicit._wakeup_channel == "jobs:wakeups"


def test_sqlspec_wakeup_channel_derives_without_changing_tables() -> None:
    pytest.importorskip("sqlspec")
    from litestar_queues.backends.sqlspec import SQLSpecBackendConfig, SQLSpecQueueBackend

    backend_config = SQLSpecBackendConfig(queue_table_name="jobs")
    backend = SQLSpecQueueBackend(
        QueueConfig(namespace="myapp", queue_backend=backend_config, worker=WorkerConfig(placement="external")),
        backend_config=backend_config,
    )

    assert backend._wakeup_channel == "myapp_tasks"
    assert backend._queue_table_name == "jobs"


def test_runtime_components_use_namespace_logger_hierarchy() -> None:
    from litestar_queues import QueuePlugin, QueueService
    from litestar_queues.backends.memory import InMemoryQueueBackend
    from litestar_queues.events import QueueEventPublisher
    from litestar_queues.worker import Worker
    from litestar_queues.worker.supervisor import ServerWorkerSupervisor

    config = QueueConfig(namespace="myapp", queue_backend="memory", worker=WorkerConfig(placement="external"))
    service = QueueService(config)
    worker = Worker(service)

    assert service._logger.name == "myapp.service"
    assert worker._logger.name == "myapp.worker"
    assert QueuePlugin(config)._logger.name == "myapp.plugin"
    assert QueueEventPublisher(namespace=config.names)._logger.name == "myapp.events.publisher"
    assert InMemoryQueueBackend(config)._logger.name == "myapp.backends.InMemoryQueueBackend"
    assert ServerWorkerSupervisor(config)._logger.name == "myapp.worker.supervisor"


def test_runtime_resource_drift_gate_keeps_legacy_literals_out_of_live_paths() -> None:
    package_root = Path(__file__).parents[2] / "litestar_queues"
    supervisor = (package_root / "worker" / "supervisor.py").read_text()
    invocation = (package_root / "worker" / "invocation.py").read_text()
    ephemeral = (package_root / "backends" / "ephemeral" / "server.py").read_text()

    assert 'name="litestar-queues-' not in supervisor
    assert "os.environ[_PROCESS_ROLE_ENV_VAR]" not in supervisor
    assert "tempfile.mkdtemp(prefix=_DIRECTORY_PREFIX)" not in invocation
    assert "tempfile.mkdtemp(prefix=_DIRECTORY_PREFIX)" not in ephemeral


def test_cloudrun_environment_defaults_derive_from_namespace_and_explicit_prefix_wins() -> None:
    from litestar_queues.execution.cloudrun import CloudRunExecutionConfig

    derived = CloudRunExecutionConfig(project_id="project", job_name="worker")
    explicit = CloudRunExecutionConfig(project_id="project", job_name="worker", env_prefix="WORK")
    namespace = QueueNamespace("myapp")

    assert derived.env_name("TASK_ID", namespace=namespace) == "QUEUES_TASK_ID"
    assert explicit.env_name("TASK_ID", namespace=namespace) == "QUEUES_TASK_ID"
    assert derived.env_name("TASK_ID") == "QUEUES_TASK_ID"

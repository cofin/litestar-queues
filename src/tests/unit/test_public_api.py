import subprocess
import sys
from pathlib import Path

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib


def test_public_exports() -> "None":
    """Test that the package exposes the public queue API.

    Optional backends (advanced_alchemy, sqlspec, redis, valkey) are NOT in the
    top-level public API and must be imported explicitly from their submodules.
    The factory imports them lazily on first lookup.
    """
    import litestar_queues
    from litestar_queues import (
        AsyncServiceProvider,
        CloudRunExecutionBackend,
        CloudRunExecutionConfig,
        CloudRunExecutionStatus,
        ExecutionBackendConfig,
        ExecutionBackendConfigProtocol,
        ImmediateExecutionBackend,
        InMemoryQueueBackend,
        LocalExecutionBackend,
        QueueBackendConfig,
        QueueBackendConfigProtocol,
        QueueConfig,
        QueuedTaskRecord,
        QueueError,
        QueuePlugin,
        QueueService,
        ScheduleConfig,
        StaleTaskRecoveryResult,
        Task,
        TaskResult,
        Worker,
        get_execution_backend_class,
        get_queue_backend_class,
        get_scheduled_tasks,
        get_task_registry,
        list_execution_backends,
        list_queue_backends,
        task,
    )

    expected_exports = {
        "AsyncServiceProvider",
        "BaseExecutionBackend",
        "BaseQueueBackend",
        "CloudRunExecutionBackend",
        "CloudRunExecutionConfig",
        "CloudRunExecutionStatus",
        "EnqueueSpec",
        "ExecutionBackendConfig",
        "ExecutionBackendConfigProtocol",
        "ImmediateExecutionBackend",
        "InMemoryQueueBackend",
        "LocalExecutionBackend",
        "QueueBackendConfig",
        "QueueBackendConfigProtocol",
        "QueuedTaskRecord",
        "QueueConfig",
        "QueueError",
        "QueuePlugin",
        "QueueService",
        "ScheduleConfig",
        "StaleTaskRecoveryResult",
        "Task",
        "TaskResult",
        "Worker",
        "discover_tasks",
        "get_execution_backend_class",
        "get_queue_backend_class",
        "get_scheduled_tasks",
        "get_task_registry",
        "list_execution_backends",
        "list_queue_backends",
        "task",
    }
    forbidden_exports = {
        "AdvancedAlchemyQueueBackend",
        "RedisBackendConfig",
        "RedisQueueBackend",
        "SQLSpecQueueBackend",
        "ValkeyBackendConfig",
        "ValkeyQueueBackend",
    }

    assert expected_exports.issubset(set(litestar_queues.__all__))
    assert forbidden_exports.isdisjoint(set(litestar_queues.__all__))
    assert QueueConfig().queue_backend == "memory"
    assert QueueBackendConfig is not None
    assert ExecutionBackendConfig is not None
    assert QueueBackendConfigProtocol is not None
    assert ExecutionBackendConfigProtocol is not None
    assert get_queue_backend_class("memory") is InMemoryQueueBackend
    assert {"advanced-alchemy", "memory", "redis", "sqlspec", "valkey"}.issubset(set(list_queue_backends()))
    assert get_execution_backend_class("cloudrun") is CloudRunExecutionBackend
    assert get_execution_backend_class("immediate") is ImmediateExecutionBackend
    assert get_execution_backend_class("local") is LocalExecutionBackend
    assert {"cloudrun", "immediate", "local"}.issubset(set(list_execution_backends()))
    assert QueuePlugin().config.queue_backend == "memory"
    assert QueuePlugin().config.execution_backend == "local"
    assert QueuePlugin().config.in_app_worker is True
    assert QueueService(QueueConfig()).config.queue_backend == "memory"
    assert QueueService(QueueConfig()).config.execution_backend == "local"
    assert issubclass(QueueError, Exception)
    assert AsyncServiceProvider(QueueConfig()) is not None
    assert ScheduleConfig(task_name="example", interval=1).task_name == "example"
    assert StaleTaskRecoveryResult().requeued == 0
    assert QueuedTaskRecord(task_name="example").task_name == "example"
    assert CloudRunExecutionBackend is not None
    assert CloudRunExecutionConfig(project_id="example", job_name="worker").resolve_job_name() == "worker"
    assert CloudRunExecutionStatus().running is True
    assert Task is not None
    assert TaskResult is not None
    assert Worker is not None
    assert get_task_registry() == {}
    assert get_scheduled_tasks() == {}
    assert callable(task)


def test_optional_backends_resolve_lazily_via_factory() -> "None":
    """Optional backends are not top-level exports but are resolvable by name through the factory."""
    from litestar_queues import get_queue_backend_class
    from litestar_queues.backends.advanced_alchemy import AdvancedAlchemyQueueBackend
    from litestar_queues.backends.redis import RedisQueueBackend
    from litestar_queues.backends.sqlspec import SQLSpecQueueBackend
    from litestar_queues.backends.valkey import ValkeyQueueBackend

    assert get_queue_backend_class("advanced-alchemy") is AdvancedAlchemyQueueBackend
    assert get_queue_backend_class("redis") is RedisQueueBackend
    assert get_queue_backend_class("sqlspec") is SQLSpecQueueBackend
    assert get_queue_backend_class("valkey") is ValkeyQueueBackend


def test_queued_task_record_normalizes_naive_scheduled_at() -> "None":
    """Queued records normalize naive scheduled datetimes before due checks."""
    from datetime import datetime, timedelta, timezone

    from litestar_queues import QueuedTaskRecord

    naive_scheduled_at = (datetime.now(timezone.utc) - timedelta(minutes=1)).replace(tzinfo=None)

    record = QueuedTaskRecord(task_name="example", scheduled_at=naive_scheduled_at)

    assert record.scheduled_at == naive_scheduled_at.replace(tzinfo=timezone.utc)
    assert record.is_due is True


def test_optional_backend_configs_live_on_submodules() -> "None":
    """Backend-specific config dataclasses are not top-level exports."""
    from litestar_queues.backends.redis import RedisBackendConfig
    from litestar_queues.backends.valkey import ValkeyBackendConfig

    assert RedisBackendConfig(url="redis://example").url == "redis://example"
    assert ValkeyBackendConfig(url="valkey://example").url == "valkey://example"


def test_task_dependency_resolver_is_re_exported_from_package_root() -> "None":
    """TaskDependencyResolver is part of the package root surface."""
    import litestar_queues
    from litestar_queues import TaskDependencyResolver
    from litestar_queues.config import TaskDependencyResolver as ConfigTaskDependencyResolver

    assert TaskDependencyResolver is ConfigTaskDependencyResolver
    assert "TaskDependencyResolver" in litestar_queues.__all__


def test_task_dependency_resolver_config_surface() -> "None":
    """The TaskDependencyResolver alias and config field are part of the config module surface."""
    from litestar_queues import config as config_module
    from litestar_queues.config import QueueConfig, TaskDependencyResolver

    assert "TaskDependencyResolver" in config_module.__all__
    assert TaskDependencyResolver is not None
    instance = QueueConfig()
    assert instance.task_dependency_resolver is None
    assert instance.signature_namespace["TaskDependencyResolver"] is TaskDependencyResolver


def test_job_cancelled_helper_is_public_and_in_signature_namespace() -> "None":
    """Cooperative cancellation is available from the package root and Litestar signature namespace."""
    import litestar_queues
    from litestar_queues import JobCancelledError, job_cancelled
    from litestar_queues.config import QueueConfig
    from litestar_queues.exceptions import JobCancelledError as ExceptionsJobCancelledError

    instance = QueueConfig()

    assert JobCancelledError is ExceptionsJobCancelledError
    assert "JobCancelledError" in litestar_queues.__all__
    assert "job_cancelled" in litestar_queues.__all__
    assert instance.signature_namespace["JobCancelledError"] is JobCancelledError
    assert instance.signature_namespace["job_cancelled"] is job_cancelled


def test_public_typing_facade_exports_optional_observability_types() -> "None":
    """The supported typing facade should expose optional observability shims."""
    from litestar_queues import typing as queue_typing

    assert isinstance(queue_typing.OPENTELEMETRY_INSTALLED, bool)
    assert isinstance(queue_typing.PROMETHEUS_INSTALLED, bool)
    assert queue_typing.OtelSpan is not None
    assert queue_typing.OtelTracer is not None
    assert queue_typing.OtelSpanKind is not None
    assert queue_typing.OtelStatus is not None
    assert queue_typing.OtelStatusCode is not None
    assert queue_typing.otel_trace is not None
    assert queue_typing.otel_propagate is not None
    assert queue_typing.OtelMeter is not None
    assert queue_typing.otel_metrics is not None
    assert queue_typing.PrometheusCounter is not None
    assert queue_typing.PrometheusGauge is not None
    assert queue_typing.PrometheusHistogram is not None
    assert not hasattr(queue_typing, "Counter")
    assert not hasattr(queue_typing, "Span")


def test_observability_optional_extras_are_declared() -> "None":
    """OpenTelemetry and Prometheus stay optional package extras."""
    pyproject = tomllib.loads(Path("pyproject.toml").read_text())

    optional_dependencies = pyproject["project"]["optional-dependencies"]
    assert optional_dependencies["otel"] == ["opentelemetry-api", "opentelemetry-sdk"]
    assert optional_dependencies["prometheus"] == ["prometheus-client"]
    assert "observability" not in optional_dependencies

    tests_dependencies = pyproject["dependency-groups"]["tests"]
    assert "opentelemetry-api" in tests_dependencies
    assert "opentelemetry-sdk" in tests_dependencies
    assert "prometheus-client" in tests_dependencies


def test_package_import_does_not_import_observability_dependencies() -> "None":
    """Base package import must not import optional telemetry modules."""
    script = """
import sys
import litestar_queues
forbidden = (
    "opentelemetry",
    "opentelemetry.trace",
    "opentelemetry.metrics",
    "prometheus_client",
)
loaded = [name for name in forbidden if name in sys.modules]
if loaded:
    raise SystemExit(",".join(loaded))
"""

    result = subprocess.run([sys.executable, "-c", script], check=False, capture_output=True, text=True)

    assert result.returncode == 0, result.stderr or result.stdout

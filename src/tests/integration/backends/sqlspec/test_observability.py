from datetime import timedelta
from typing import TYPE_CHECKING, Any, cast

import pytest

pytest.importorskip("aiosqlite")
pytest.importorskip("sqlspec")

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.observability import ObservabilityConfig, StatementEvent
from sqlspec.utils.correlation import CorrelationContext

from litestar_queues import QueueConfig
from litestar_queues.backends.sqlspec import SQLSpecBackendConfig, SQLSpecQueueBackend
from litestar_queues.observability import QueueObservabilityConfig

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.anyio


class RecordingSpanManager:
    __slots__ = ("ended", "is_enabled", "started")

    def __init__(self) -> "None":
        self.is_enabled = True
        self.started: "list[tuple[str, dict[str, Any]]]" = []
        self.ended: "list[tuple[tuple[str, dict[str, Any]] | None, Exception | None]]" = []

    def start_span(self, name: "str", attributes: "dict[str, Any] | None" = None) -> "tuple[str, dict[str, Any]]":
        span = (name, dict(attributes or {}))
        self.started.append(span)
        return span

    def start_query_span(
        self,
        *,
        driver: "str",
        adapter: "str",
        bind_key: "str | None",
        sql: "str",
        operation: "str",
        connection_info: "dict[str, Any]",
        correlation_id: "str | None",
    ) -> "tuple[str, dict[str, Any]]":
        attributes = {
            "db.operation": operation,
            "db.statement": sql,
            "sqlspec.adapter": adapter,
            "sqlspec.driver": driver,
            **connection_info,
        }
        if bind_key is not None:
            attributes["sqlspec.bind_key"] = bind_key
        if correlation_id is not None:
            attributes["sqlspec.correlation_id"] = correlation_id
        return self.start_span("sqlspec.query", attributes)

    def end_span(self, span: "tuple[str, dict[str, Any]] | None", *, error: "Exception | None" = None) -> "None":
        self.ended.append((span, error))


async def test_sqlspec_backend_emits_queue_metrics_spans_and_correlation(tmp_path: "Path") -> "None":
    statement_events: "list[StatementEvent]" = []
    lifecycle_events: "list[dict[str, Any]]" = []
    sqlspec_config = AiosqliteConfig(
        connection_config={"database": str(tmp_path / "queue.db")},
        observability_config=ObservabilityConfig(statement_observers=(statement_events.append,)),
    )
    backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(
            config=sqlspec_config, event_channel=cast("Any", StubEventChannel()), notifications=True
        )
    )
    await backend.open()
    runtime = sqlspec_config.get_observability_runtime()
    runtime.register_lifecycle_hook("on_query_complete", lifecycle_events.append)
    span_manager = RecordingSpanManager()
    runtime.span_manager = span_manager
    try:
        with CorrelationContext.context("queue-correlation"):
            await _exercise_observed_queue_operations(backend)

        metrics = runtime.metrics_snapshot()
        prefix = runtime.diagnostics_key
        assert metrics[f"{prefix}.queue.enqueue"] == 4.0
        assert metrics[f"{prefix}.queue.notify"] == 4.0
        assert metrics[f"{prefix}.queue.claim"] == 4.0
        assert metrics[f"{prefix}.queue.complete"] == 1.0
        assert metrics[f"{prefix}.queue.retry"] == 1.0
        assert metrics[f"{prefix}.queue.fail"] == 1.0
        assert metrics[f"{prefix}.queue.stale_recovered"] == 1.0
        assert metrics[f"{prefix}.queue.stale_failed"] == 1.0
        assert metrics[f"{prefix}.queue.claim_lost"] == 1.0
        assert any(event.correlation_id == "queue-correlation" for event in statement_events)
        assert any(event["correlation_id"] == "queue-correlation" for event in lifecycle_events)
        assert any(
            name == "sqlspec.queue.enqueue" and attributes["sqlspec.correlation_id"] == "queue-correlation"
            for name, attributes in span_manager.started
        )
        assert any(
            name == "sqlspec.query" and attributes["sqlspec.correlation_id"] == "queue-correlation"
            for name, attributes in span_manager.started
        )
    finally:
        await backend.close()


async def _exercise_observed_queue_operations(backend: "SQLSpecQueueBackend") -> "None":
    completed_seed = await backend.enqueue("tasks.observed.complete")
    completed_claim = await backend.claim_task(completed_seed.id)
    assert completed_claim is not None
    completed = await backend.complete_task(
        completed_claim.id, result={"ok": True}, expected_retry_count=completed_claim.retry_count
    )
    assert completed is not None

    retry_seed = await backend.enqueue("tasks.observed.retry", max_retries=1)
    retry_claim = await backend.claim_task(retry_seed.id)
    assert retry_claim is not None
    retried = await backend.fail_task(retry_claim.id, "retryable", expected_retry_count=retry_claim.retry_count)
    assert retried is not None
    assert retried.status == "pending"

    failed_seed = await backend.enqueue("tasks.observed.fail", max_retries=0)
    failed_claim = await backend.claim_task(failed_seed.id)
    assert failed_claim is not None
    failed = await backend.fail_task(failed_claim.id, "terminal", expected_retry_count=failed_claim.retry_count)
    assert failed is not None
    assert failed.status == "failed"

    stale_seed = await backend.enqueue("tasks.observed.stale", max_retries=0, metadata={"requeue_on_stale": False})
    stale_claim = await backend.claim_task(stale_seed.id)
    assert stale_claim is not None
    stale_result = await backend.requeue_stale_running(stale_after=timedelta(seconds=-1))
    assert stale_result.failed == 1
    assert stale_result.handler_needed == 1
    assert (
        await backend.complete_task(stale_claim.id, result="too late", expected_retry_count=stale_claim.retry_count)
        is None
    )


async def test_sqlspec_backend_can_disable_queue_domain_observability(tmp_path: "Path") -> "None":
    statement_events: "list[StatementEvent]" = []
    sqlspec_config = AiosqliteConfig(
        connection_config={"database": str(tmp_path / "queue-disabled.db")},
        observability_config=ObservabilityConfig(statement_observers=(statement_events.append,)),
    )
    backend = SQLSpecQueueBackend(backend_config=SQLSpecBackendConfig(config=sqlspec_config, queue_observability=False))
    await backend.open()
    try:
        with CorrelationContext.context("queue-observability-disabled"):
            await backend.enqueue("tasks.observed.disabled")

        metrics = sqlspec_config.get_observability_runtime().metrics_snapshot()
        assert not any(".queue." in name for name in metrics)
        assert any(event.correlation_id == "queue-observability-disabled" for event in statement_events)
    finally:
        await backend.close()


async def test_package_observability_disables_sqlspec_queue_domain_observability(tmp_path: "Path") -> "None":
    statement_events: "list[StatementEvent]" = []
    sqlspec_config = AiosqliteConfig(
        connection_config={"database": str(tmp_path / "queue-package-observability.db")},
        observability_config=ObservabilityConfig(statement_observers=(statement_events.append,)),
    )
    queue_config = QueueConfig(observability_config=QueueObservabilityConfig(enable_otel=True))
    backend = SQLSpecQueueBackend(config=queue_config, backend_config=SQLSpecBackendConfig(config=sqlspec_config))
    await backend.open()
    try:
        with CorrelationContext.context("package-queue-observability"):
            await backend.enqueue("tasks.observed.package")

        metrics = sqlspec_config.get_observability_runtime().metrics_snapshot()
        assert not any(".queue." in name for name in metrics)
        assert any(event.correlation_id == "package-queue-observability" for event in statement_events)
    finally:
        await backend.close()


class StubEventChannel:
    __slots__ = ("_backend_name", "published")

    def __init__(self) -> "None":
        self._backend_name = "table_queue"
        self.published: "list[tuple[str, dict[str, object], dict[str, object] | None]]" = []

    async def publish(
        self, channel: "str", payload: "dict[str, object]", metadata: "dict[str, object] | None" = None
    ) -> "str":
        self.published.append((channel, payload, metadata))
        return f"event-{len(self.published)}"

    async def shutdown(self) -> "None":
        return None

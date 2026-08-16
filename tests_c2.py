import contextlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


def _recording_provider(
    events: "list[str]",
    *,
    acquire_error: "BaseException | None" = None,
    cleanup_error: "BaseException | None" = None,
    payload: "Mapping[str, object] | None" = None,
) -> "TaskDependencyProvider":
    """Build a provider that appends an ordered trace to ``events``."""

    @contextlib.asynccontextmanager
    async def provider(
        task: "Task[..., object]", record: "QueuedTaskRecord", context: "TaskExecutionContext"
    ) -> "AsyncIterator[Mapping[str, object]]":
        events.append("acquire")
        if acquire_error is not None:
            raise acquire_error
        try:
            yield dict(payload or {"scoped": "value"})
        finally:
            events.append("cleanup")
            if cleanup_error is not None:
                raise cleanup_error

    return provider


async def test_provider_scope_wraps_successful_attempt() -> "None":
    from litestar_queues import QueueConfig, QueueService, WorkerConfig, task
    from litestar_queues.task import clear_task_registry

    clear_task_registry()
    events: list[str] = []

    @task("scoped.success")
    async def run(scoped: str) -> str:
        assert scoped == "value"
        events.append("body")
        return "done"

    config = QueueConfig(
        worker=WorkerConfig(placement="external"),
        queue_backend="memory",
        task_dependency_provider=_recording_provider(events),
    )
    async with QueueService(config) as service:
        result = await service.enqueue("scoped.success")

    await result.refresh()
    assert events == ["acquire", "body", "cleanup"]
    assert result.status == "completed"


async def test_provider_scope_closes_on_retryable_body_failure() -> "None":
    from litestar_queues import QueueConfig, QueueService, WorkerConfig, task
    from litestar_queues.task import clear_task_registry

    clear_task_registry()
    events: list[str] = []

    @task("scoped.retryable")
    async def run(scoped: str) -> str:
        events.append("body")
        raise RuntimeError("boom")

    config = QueueConfig(
        worker=WorkerConfig(placement="external"),
        queue_backend="memory",
        task_dependency_provider=_recording_provider(events),
    )
    async with QueueService(config) as service:
        result = await service.enqueue("scoped.retryable", retries=1)

    await result.refresh()
    assert events == ["acquire", "body", "cleanup"]
    assert result.status in {"pending", "scheduled"}
    assert result.error
    assert "boom" in result.error


async def test_provider_scope_closes_on_terminal_body_failure() -> "None":
    from litestar_queues import QueueConfig, QueueService, WorkerConfig, task
    from litestar_queues.exceptions import NonRetryableError
    from litestar_queues.task import clear_task_registry

    clear_task_registry()
    events: list[str] = []

    @task("scoped.terminal")
    async def run(scoped: str) -> str:
        events.append("body")
        raise NonRetryableError("nope")

    config = QueueConfig(
        worker=WorkerConfig(placement="external"),
        queue_backend="memory",
        task_dependency_provider=_recording_provider(events),
    )
    async with QueueService(config) as service:
        result = await service.enqueue("scoped.terminal")

    await result.refresh()
    assert events == ["acquire", "body", "cleanup"]
    assert result.status == "failed"


async def test_provider_acquisition_failure_never_cleans_up() -> "None":
    from litestar_queues import QueueConfig, QueueService, WorkerConfig, task
    from litestar_queues.task import clear_task_registry

    clear_task_registry()
    events: list[str] = []

    @task("scoped.acquire_fail")
    async def run(scoped: str) -> str:
        events.append("body")
        return "done"

    config = QueueConfig(
        worker=WorkerConfig(placement="external"),
        queue_backend="memory",
        task_dependency_provider=_recording_provider(events, acquire_error=RuntimeError("no session")),
    )
    async with QueueService(config) as service:
        result = await service.enqueue("scoped.acquire_fail", retries=1)

    await result.refresh()
    assert events == ["acquire"]
    assert result.status in {"pending", "scheduled"}
    assert result.retry_count == 1
    assert result.error
    assert "no session" in result.error


async def test_provider_scope_receives_task_record_and_context() -> "None":

    from litestar_queues import QueueConfig, QueueService, WorkerConfig, task
    from litestar_queues.task import clear_task_registry

    clear_task_registry()

    captured_args = {}

    @contextlib.asynccontextmanager
    async def provider(task_obj: "object", record: "object", context: "object") -> "AsyncIterator[dict[str, object]]":
        captured_args["task"] = task_obj
        captured_args["record"] = record
        captured_args["context"] = context
        yield {}

    @task("scoped.args")
    async def run() -> str:
        return "done"

    config = QueueConfig(
        worker=WorkerConfig(placement="external"), queue_backend="memory", task_dependency_provider=provider
    )
    async with QueueService(config) as service:
        result = await service.enqueue("scoped.args")

    await result.refresh()
    assert captured_args["task"].name == "scoped.args"
    assert captured_args["record"].id == result.id
    assert captured_args["context"].task_id == result.id


async def test_provider_output_never_overrides_task_context() -> "None":
    from litestar_queues import QueueConfig, QueueService, TaskExecutionContext, WorkerConfig, task
    from litestar_queues.task import clear_task_registry

    clear_task_registry()
    events: list[str] = []

    @task("scoped.override")
    async def run(_task_context: "TaskExecutionContext") -> str:
        assert isinstance(_task_context, TaskExecutionContext)
        events.append("body")
        return "done"

    config = QueueConfig(
        worker=WorkerConfig(placement="external"),
        queue_backend="memory",
        task_dependency_provider=_recording_provider(events, payload={"_task_context": "hijacked"}),
    )
    async with QueueService(config) as service:
        result = await service.enqueue("scoped.override")

    await result.refresh()
    assert events == ["acquire", "body", "cleanup"]

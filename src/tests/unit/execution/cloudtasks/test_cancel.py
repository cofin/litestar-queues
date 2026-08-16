from datetime import datetime, timezone
from uuid import uuid4

import pytest

from litestar_queues.backends.memory.backend import InMemoryQueueBackend
from litestar_queues.config import QueueConfig, WorkerConfig
from litestar_queues.execution.cloudtasks.backend import CloudTasksExecutionBackend
from litestar_queues.execution.cloudtasks.config import CloudTasksExecutionConfig
from litestar_queues.models import QueuedTaskRecord
from litestar_queues.service import QueueService
from tests.unit.execution.cloudtasks._fakes import FakeCloudTasksClient


@pytest.mark.anyio
async def test_cloudtasks_cancel_execution_deletes_the_stored_delivery() -> "None":
    task_name = "projects/test/locations/us/queues/default/tasks/123"
    client = FakeCloudTasksClient(existing={task_name})
    backend = CloudTasksExecutionBackend(
        execution_config=CloudTasksExecutionConfig(
            project_id="test",
            location="us",
            queue_id="default",
            service_url="https://consumer.example.run.app",
            service_account_email="test@test.com",
            trust_platform_auth=True,
        ),
        client=client,
    )
    record = QueuedTaskRecord(
        id=uuid4(),
        task_name="tasks.foo",
        status="running",
        queued_at=datetime.now(timezone.utc),
        execution_ref=task_name,
        queue="default",
        execution_backend="cloudtasks",
        retry_count=0,
        max_retries=3,
        kwargs={},
        execution_profile=None,
    )
    result = await backend.cancel_execution(None, record)  # type: ignore
    assert result.status == "accepted"
    assert client.delete_calls == [task_name]
    assert client.get_calls == []


@pytest.mark.anyio
async def test_cloudtasks_cancel_execution_missing_delivery_is_idempotent() -> "None":
    task_name = "projects/test/locations/us/queues/default/tasks/123"
    client = FakeCloudTasksClient(existing=set())
    backend = CloudTasksExecutionBackend(
        execution_config=CloudTasksExecutionConfig(
            project_id="test",
            location="us",
            queue_id="default",
            service_url="https://consumer.example.run.app",
            service_account_email="test@test.com",
            trust_platform_auth=True,
        ),
        client=client,
    )
    record = QueuedTaskRecord(
        id=uuid4(),
        task_name="tasks.foo",
        status="running",
        queued_at=datetime.now(timezone.utc),
        execution_ref=task_name,
        queue="default",
        execution_backend="cloudtasks",
        retry_count=0,
        max_retries=3,
        kwargs={},
        execution_profile=None,
    )
    result = await backend.cancel_execution(None, record)  # type: ignore
    assert result.status == "already_cancelled"
    assert client.delete_calls == [task_name]


@pytest.mark.anyio
async def test_cloudtasks_cancel_execution_api_error_is_retryable() -> "None":
    task_name = "projects/test/locations/us/queues/default/tasks/123"

    class ErrorClient(FakeCloudTasksClient):
        async def delete_task(self, *, name: "str", timeout: "float | None" = None) -> "None":
            msg = "unavailable"
            raise RuntimeError(msg)

    client = ErrorClient(existing={task_name})
    backend = CloudTasksExecutionBackend(
        execution_config=CloudTasksExecutionConfig(
            project_id="test",
            location="us",
            queue_id="default",
            service_url="https://consumer.example.run.app",
            service_account_email="test@test.com",
            trust_platform_auth=True,
        ),
        client=client,
    )
    record = QueuedTaskRecord(
        id=uuid4(),
        task_name="tasks.foo",
        status="running",
        queued_at=datetime.now(timezone.utc),
        execution_ref=task_name,
        queue="default",
        execution_backend="cloudtasks",
        retry_count=0,
        max_retries=3,
        kwargs={},
        execution_profile=None,
    )
    result = await backend.cancel_execution(None, record)  # type: ignore
    assert result.status == "retryable"
    assert "unavailable" in (result.detail or "")


@pytest.mark.anyio
async def test_cloudtasks_cancel_execution_without_a_reference_is_unsupported() -> "None":
    client = FakeCloudTasksClient()
    backend = CloudTasksExecutionBackend(
        execution_config=CloudTasksExecutionConfig(
            project_id="test",
            location="us",
            queue_id="default",
            service_url="https://consumer.example.run.app",
            service_account_email="test@test.com",
            trust_platform_auth=True,
        ),
        client=client,
    )
    record = QueuedTaskRecord(
        id=uuid4(),
        task_name="tasks.foo",
        status="running",
        queued_at=datetime.now(timezone.utc),
        execution_ref=None,
        queue="default",
        execution_backend="cloudtasks",
        retry_count=0,
        max_retries=3,
        kwargs={},
        execution_profile=None,
    )
    result = await backend.cancel_execution(None, record)  # type: ignore
    assert result.status == "unsupported"
    assert client.delete_calls == []


@pytest.mark.anyio
async def test_cancel_task_deletes_a_cloudtasks_delivery_before_the_durable_write() -> "None":
    from litestar_queues.task import task

    @task("tasks.ext")
    async def _ext() -> "None":
        return None

    client = FakeCloudTasksClient()
    backend = CloudTasksExecutionBackend(
        execution_config=CloudTasksExecutionConfig(
            project_id="test",
            location="us",
            queue_id="default",
            service_url="https://consumer.example.run.app",
            service_account_email="test@test.com",
            trust_platform_auth=True,
        ),
        client=client,
    )
    queue_backend = InMemoryQueueBackend()

    from unittest.mock import patch

    with patch.object(QueueConfig, "_validate_placement"):
        service = QueueService(
            QueueConfig(
                worker=WorkerConfig(placement="external"),
                queue_backend="memory",
                execution_backend=backend.execution_config,
            ),
            queue_backend=queue_backend,
            execution_backend=backend,
        )

    async with service:
        record = await queue_backend.enqueue("tasks.ext", execution_backend="cloudtasks")
        await backend.schedule(service, record)

        updated = await queue_backend.get_task(record.id)
        assert updated and updated.execution_ref

        assert await service.cancel_task(record.id, include_running=True) is True
        assert client.delete_calls == [updated.execution_ref]

        stored = await queue_backend.get_task(record.id)
        assert stored and stored.status == "cancelled"

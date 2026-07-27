"""The request the package builds is one Google actually accepts.

The producer builds its CreateTask request as a plain mapping so unit tests can
read it without the ``cloud-tasks`` extra. That freedom costs a guarantee: a
fake will happily accept a field Google renamed or retyped. These tests feed the
real request through the real message classes, which is the only place a drift
between the two shows up before a deployment does.
"""

from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

import pytest

from litestar_queues.execution.cloudtasks import CloudTasksExecutionConfig
from litestar_queues.execution.cloudtasks.backend import _create_task_request
from litestar_queues.models import QueuedTaskRecord

tasks_v2 = pytest.importorskip("google.cloud.tasks_v2", reason="requires the cloud-tasks extra")

DELIVERY_NAME_SUFFIX = "tasks/lq-delivery"


@pytest.fixture
def execution_config() -> "CloudTasksExecutionConfig":
    """A valid Cloud Tasks configuration.

    Returns:
        The configuration under test.
    """
    return CloudTasksExecutionConfig(
        project_id="example-project",
        location="us-central1",
        queue_id="queue-consumer",
        service_url="https://queue-consumer-abcdef-uc.a.run.app",
        service_account_email="queues@example-project.iam.gserviceaccount.com",
        trust_platform_auth=True,
    )


def _build(config: "CloudTasksExecutionConfig", record: "QueuedTaskRecord") -> "Any":
    """Convert the package's request through the real Google message class.

    Returns:
        The converted ``CreateTaskRequest``.
    """
    request = _create_task_request(config, record, f"{config.queue_path}/{DELIVERY_NAME_SUFFIX}")
    return tasks_v2.CreateTaskRequest(request)


def test_every_field_survives_conversion_to_the_google_request(execution_config: "CloudTasksExecutionConfig") -> "None":
    """Plain datetimes, timedeltas, and enum names all convert without a helper."""
    due = datetime.now(timezone.utc) + timedelta(hours=1)
    record = QueuedTaskRecord(task_name="probe", execution_backend="cloudtasks", scheduled_at=due)

    built = _build(execution_config, record)

    assert built.parent == execution_config.queue_path
    assert built.task.name == f"{execution_config.queue_path}/{DELIVERY_NAME_SUFFIX}"
    assert built.task.schedule_time == due
    assert built.task.dispatch_deadline == timedelta(seconds=execution_config.dispatch_deadline)
    assert built.task.http_request.http_method == tasks_v2.HttpMethod.POST
    assert built.task.http_request.url == execution_config.target_url
    assert dict(built.task.http_request.headers) == {"Content-Type": "application/json"}
    assert built.task.http_request.oidc_token.service_account_email == execution_config.service_account_email
    assert built.task.http_request.oidc_token.audience == execution_config.audience


def test_the_delivered_body_is_the_bytes_the_package_encoded(execution_config: "CloudTasksExecutionConfig") -> "None":
    """Conversion must not re-encode the body; the consumer parses these bytes."""
    record = QueuedTaskRecord(task_name="probe", execution_backend="cloudtasks", id=uuid4())

    built = _build(execution_config, record)

    assert built.task.http_request.body == b'{"version":1,"task_id":"' + str(record.id).encode() + b'"}'


def test_an_undated_record_leaves_the_schedule_time_unset(execution_config: "CloudTasksExecutionConfig") -> "None":
    """An unset schedule time is what makes Google dispatch on its own clock."""
    record = QueuedTaskRecord(task_name="probe", execution_backend="cloudtasks")

    built = _build(execution_config, record)

    assert "schedule_time" not in tasks_v2.Task.to_dict(built.task)

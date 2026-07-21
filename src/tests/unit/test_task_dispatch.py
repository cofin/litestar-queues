"""Unit tests for the universal task dispatch."""

import pytest


def test_dispatch_round_trips_through_json() -> "None":
    from litestar_queues.execution.dispatch import TaskDispatch

    dispatch = TaskDispatch(
        task_id="0f9c",
        task_name="tasks.remote",
        queue="default",
        execution_backend="cloudrun",
        args=(41,),
        kwargs={"flag": True},
        execution_profile="heavy",
    )

    restored = TaskDispatch.from_json(dispatch.to_json())

    assert restored == dispatch
    assert restored.args == (41,)


def test_from_record_projects_the_subset() -> "None":
    from litestar_queues.execution.dispatch import TASK_DISPATCH_VERSION, TaskDispatch
    from litestar_queues.models import QueuedTaskRecord

    record = QueuedTaskRecord(
        task_name="tasks.remote",
        args=(1, 2),
        kwargs={"flag": True},
        queue="reports",
        execution_backend="cloudrun",
        execution_profile="heavy",
    )

    dispatch = TaskDispatch.from_record(record)

    assert dispatch.task_id == str(record.id)
    assert dispatch.task_name == "tasks.remote"
    assert dispatch.queue == "reports"
    assert dispatch.execution_backend == "cloudrun"
    assert dispatch.args == (1, 2)
    assert dispatch.kwargs == {"flag": True}
    assert dispatch.execution_profile == "heavy"
    assert dispatch.version == TASK_DISPATCH_VERSION


def test_wire_keys_are_camel_case() -> "None":
    from litestar_queues.execution.dispatch import TaskDispatch

    dispatch = TaskDispatch(
        task_id="abc",
        task_name="tasks.remote",
        queue="default",
        execution_backend="cloudrun",
        execution_profile="heavy",
    )

    data = dispatch.to_dict()

    assert "taskId" in data
    assert "taskName" in data
    assert "executionBackend" in data
    assert "executionProfile" in data


def test_from_dict_accepts_absent_version_as_current() -> "None":
    from litestar_queues.execution.dispatch import TASK_DISPATCH_VERSION, TaskDispatch

    dispatch = TaskDispatch.from_dict({
        "taskId": "abc",
        "taskName": "tasks.remote",
        "queue": "default",
        "executionBackend": "cloudrun",
    })

    assert dispatch.version == TASK_DISPATCH_VERSION


def test_from_json_rejects_unsupported_version() -> "None":
    from litestar_queues.execution.dispatch import TaskDispatch

    payload = (
        b'{"taskId":"abc","taskName":"tasks.remote","queue":"default","executionBackend":"cloudrun","version":999}'
    )

    with pytest.raises(ValueError, match="Unsupported task dispatch version"):
        TaskDispatch.from_json(payload)


def test_from_json_rejects_non_object() -> "None":
    from litestar_queues.execution.dispatch import TaskDispatch

    with pytest.raises(TypeError, match="must decode to an object"):
        TaskDispatch.from_json(b"[]")


def test_dispatch_carries_no_retry_or_result_state() -> "None":
    from litestar_queues.execution.dispatch import TaskDispatch

    assert not hasattr(TaskDispatch, "retry_count")
    for forbidden in ("retry_count", "result", "heartbeat_at"):
        assert forbidden not in TaskDispatch.__struct_fields__

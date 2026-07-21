"""Unit tests for the universal dispatch envelope."""

import pytest


def test_envelope_round_trips_through_json() -> "None":
    from litestar_queues.execution.envelope import DispatchEnvelope

    envelope = DispatchEnvelope(
        task_id="0f9c",
        task_name="tasks.remote",
        queue="default",
        execution_backend="cloudrun",
        args=(41,),
        kwargs={"flag": True},
        execution_profile="heavy",
    )

    restored = DispatchEnvelope.from_json(envelope.to_json())

    assert restored == envelope
    assert restored.args == (41,)


def test_from_record_projects_the_subset() -> "None":
    from litestar_queues.execution.envelope import DISPATCH_ENVELOPE_VERSION, DispatchEnvelope
    from litestar_queues.models import QueuedTaskRecord

    record = QueuedTaskRecord(
        task_name="tasks.remote",
        args=(1, 2),
        kwargs={"flag": True},
        queue="reports",
        execution_backend="cloudrun",
        execution_profile="heavy",
    )

    envelope = DispatchEnvelope.from_record(record)

    assert envelope.task_id == str(record.id)
    assert envelope.task_name == "tasks.remote"
    assert envelope.queue == "reports"
    assert envelope.execution_backend == "cloudrun"
    assert envelope.args == (1, 2)
    assert envelope.kwargs == {"flag": True}
    assert envelope.execution_profile == "heavy"
    assert envelope.version == DISPATCH_ENVELOPE_VERSION


def test_wire_keys_are_camel_case() -> "None":
    from litestar_queues.execution.envelope import DispatchEnvelope

    envelope = DispatchEnvelope(
        task_id="abc",
        task_name="tasks.remote",
        queue="default",
        execution_backend="cloudrun",
        execution_profile="heavy",
    )

    data = envelope.to_dict()

    assert "taskId" in data
    assert "taskName" in data
    assert "executionBackend" in data
    assert "executionProfile" in data


def test_from_dict_accepts_absent_version_as_current() -> "None":
    from litestar_queues.execution.envelope import DISPATCH_ENVELOPE_VERSION, DispatchEnvelope

    envelope = DispatchEnvelope.from_dict({
        "taskId": "abc",
        "taskName": "tasks.remote",
        "queue": "default",
        "executionBackend": "cloudrun",
    })

    assert envelope.version == DISPATCH_ENVELOPE_VERSION


def test_from_json_rejects_unsupported_version() -> "None":
    from litestar_queues.execution.envelope import DispatchEnvelope

    payload = (
        b'{"taskId":"abc","taskName":"tasks.remote","queue":"default","executionBackend":"cloudrun","version":999}'
    )

    with pytest.raises(ValueError, match="Unsupported dispatch envelope version"):
        DispatchEnvelope.from_json(payload)


def test_from_json_rejects_non_object() -> "None":
    from litestar_queues.execution.envelope import DispatchEnvelope

    with pytest.raises(TypeError, match="must decode to an object"):
        DispatchEnvelope.from_json(b"[]")


def test_envelope_carries_no_retry_or_result_state() -> "None":
    from litestar_queues.execution.envelope import DispatchEnvelope

    assert not hasattr(DispatchEnvelope, "retry_count")
    for forbidden in ("retry_count", "result", "heartbeat_at"):
        assert forbidden not in DispatchEnvelope.__struct_fields__

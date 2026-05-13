import json
import subprocess
import sys

import pytest

from litestar_queues.events import QueueEvent

pytestmark = pytest.mark.anyio


async def test_queue_event_serialization_preserves_null_keys() -> None:
    event = QueueEvent(
        type="task.progress",
        scope="task",
        scope_key=None,
        task_id="task-1",
        task_name="tasks.export",
        queue="default",
        worker_id=None,
        execution_backend="local",
        execution_profile=None,
        attempt=1,
        sequence=None,
        level=None,
        message=None,
        progress_current=None,
        progress_total=10,
        progress_percent=None,
        payload={"workspace_id": None, "stage": "extract"},
    )

    data = event.to_dict()
    decoded = json.loads(event.to_json())

    assert data["scopeKey"] is None
    assert data["workerId"] is None
    assert data["progressCurrent"] is None
    assert data["progressPercent"] is None
    assert data["payload"] == {"workspace_id": None, "stage": "extract"}
    assert decoded["scopeKey"] is None
    assert decoded["payload"]["workspace_id"] is None
    assert QueueEvent.from_json(event.to_json()).to_dict() == data


async def test_queue_event_serialization_uses_camelcase_wire_format() -> None:
    """Top-level keys are camelCase; legacy snake_case keys are absent."""
    event = QueueEvent(
        type="task.progress",
        scope="task",
        id="evt-1",
        task_id="task-123",
        task_name="tasks.run",
        scope_key="scope-1",
        execution_backend="local",
        execution_profile=None,
        progress_current=10,
        progress_total=100,
        progress_percent=10.0,
        event_key="dedup-1",
    )

    data = event.to_dict()

    assert data["taskId"] == "task-123"
    assert data["taskName"] == "tasks.run"
    assert data["scopeKey"] == "scope-1"
    assert data["executionBackend"] == "local"
    assert data["executionProfile"] is None
    assert data["progressCurrent"] == 10
    assert data["progressTotal"] == 100
    assert data["progressPercent"] == pytest.approx(10.0)
    assert data["eventKey"] == "dedup-1"

    for snake_key in ("task_id", "task_name", "scope_key", "execution_backend", "event_key"):
        assert snake_key not in data


async def test_queue_event_payload_keys_are_not_camelized() -> None:
    """User-supplied payload contents are passed through verbatim."""
    event = QueueEvent(
        type="task.event",
        scope="task",
        payload={"snake_inner": 1, "nested": {"deep_key": 2}},
    )
    data = event.to_dict()
    assert data["payload"] == {"snake_inner": 1, "nested": {"deep_key": 2}}


async def test_queue_event_round_trip_preserves_event_key() -> None:
    """event_key survives to_json -> from_json round trip."""
    event = QueueEvent(
        type="task.completed",
        scope="task",
        task_id="t-1",
        event_key="dedup-xyz",
    )
    encoded = event.to_json()

    encoded_text = encoded.decode() if isinstance(encoded, (bytes, bytearray)) else encoded
    assert '"eventKey":"dedup-xyz"' in encoded_text

    restored = QueueEvent.from_json(encoded)
    assert restored.event_key == "dedup-xyz"
    assert restored.task_id == "t-1"


async def test_queue_event_occurred_at_uses_rfc3339_with_trailing_z() -> None:
    """OccurredAt serializes to RFC 3339 UTC with a trailing Z so subscribers can rely on the format."""
    event = QueueEvent(type="task.started", scope="task")
    decoded = json.loads(event.to_json())
    assert decoded["occurredAt"].endswith("Z")


async def test_queue_event_actor_and_entity_serialize_as_camelcase_dicts() -> None:
    """Nested QueueEventActor / QueueEventEntityRef appear as native dicts on the wire."""
    from litestar_queues.events import QueueEventActor, QueueEventEntityRef

    event = QueueEvent(
        type="task.completed",
        scope="task",
        actor=QueueEventActor(type="user", id="u-1", name="Alice"),
        entity=QueueEventEntityRef(type="report", id="r-1", name="weekly"),
    )
    data = event.to_dict()
    assert isinstance(data["actor"], dict)
    assert data["actor"] == {"type": "user", "id": "u-1", "name": "Alice"}
    assert data["entity"] == {"type": "report", "id": "r-1", "name": "weekly"}


def test_queue_event_imports_do_not_load_sqlspec() -> None:
    """Importing the events module must not trigger optional sqlspec runtime."""
    code = """
import sys
import litestar_queues.events
from litestar_queues.events.models import QueueEvent  # access models surface
QueueEvent(type='task.progress', scope='task')  # construct
print('sqlspec loaded:', 'sqlspec' in sys.modules)
raise SystemExit(1 if 'sqlspec' in sys.modules else 0)
"""
    result = subprocess.run([sys.executable, "-c", code], check=False, capture_output=True, text=True)
    assert result.returncode == 0, result.stdout


async def test_queue_event_supports_event_key_field() -> None:
    """QueueEvent has an event_key field that defaults to None and is settable."""
    default_event = QueueEvent(type="task.progress", scope="task")
    assert default_event.event_key is None

    keyed_event = QueueEvent(type="task.progress", scope="task", event_key="dedup-1")
    assert keyed_event.event_key == "dedup-1"

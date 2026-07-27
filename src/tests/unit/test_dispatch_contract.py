"""The one rule every external transport has to keep.

A transport carries a task id. Not arguments, not metadata, not retry state, not
an envelope describing any of those — the id, and the protocol version that says
how to read it. The consumer re-reads everything else from the queue store,
which is what makes the queue's own record authoritative rather than whatever a
message happened to be carrying when it was written.

This is easy to state and easy to lose. Each new broker arrives with its own
serialization format and its own natural place to stash "just one more field",
and the first one that does it turns the queue store into a cache of the
transport. These assertions are what a future backend has to get past.

They read source and public docs only. The roadmap and the plans live in the
project's own tracker, and a runtime test that parsed them would fail for people
who never had them.
"""

from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest
from litestar.serialization import decode_json

if TYPE_CHECKING:
    from collections.abc import Iterator

    from litestar_queues.execution.base import BaseExecutionBackend

pytestmark = pytest.mark.anyio

SOURCE = Path("src/litestar_queues")
DOCS = Path("docs")

SECRET_ARGUMENT = "sk-must-never-cross-the-wire"


def _published_text() -> "Iterator[tuple[Path, str]]":
    """Every source and documentation file a reader or a byte can reach.

    Yields:
        Each path and its text.
    """
    for root, suffix in ((SOURCE, "*.py"), (DOCS, "*.rst")):
        for path in sorted(root.rglob(suffix)):
            if "_build" in path.parts:
                continue
            yield path, path.read_text(encoding="utf-8")


def test_no_source_or_public_doc_revives_the_removed_envelope() -> "None":
    """It was replaced by the id, and a plan that still names it will be followed."""
    offenders = [str(path) for path, text in _published_text() if "DispatchEnvelope" in text]

    assert offenders == []


def test_the_delivery_body_has_exactly_two_fields() -> "None":
    """Anything else on the wire is either ignored or, worse, believed."""
    from litestar_queues.execution.cloudtasks.routes import CloudTasksDelivery

    assert CloudTasksDelivery.__struct_fields__ == ("version", "task_id")
    assert CloudTasksDelivery.__struct_config__.forbid_unknown_fields is True


def test_no_argument_metadata_or_retry_state_reaches_the_transport() -> "None":
    """The record in storage is authoritative; the message only locates it."""
    from litestar_queues.execution.cloudtasks import CloudTasksExecutionConfig
    from litestar_queues.execution.cloudtasks.backend import _create_task_request
    from litestar_queues.models import QueuedTaskRecord

    record = QueuedTaskRecord(
        task_name="probe",
        execution_backend="cloudtasks",
        args=(SECRET_ARGUMENT,),
        kwargs={"api_key": SECRET_ARGUMENT},
        metadata={"tenant": SECRET_ARGUMENT},
        retry_count=3,
        id=uuid4(),
    )
    config = CloudTasksExecutionConfig(
        project_id="example-project",
        location="us-central1",
        queue_id="queue-consumer",
        service_url="https://queue-consumer-abcdef-uc.a.run.app",
        service_account_email="queues@example-project.iam.gserviceaccount.com",
        trust_platform_auth=True,
    )

    body = _create_task_request(config, record, f"{config.queue_path}/tasks/lq-probe")["task"]["http_request"]["body"]

    assert decode_json(body) == {"version": 1, "task_id": str(record.id)}
    assert SECRET_ARGUMENT not in body.decode()
    assert b"retry_count" not in body
    assert b"probe" not in body


def test_the_delivered_id_is_the_only_thing_the_consumer_is_given() -> "None":
    """Every transport routes through one core, so there is one place to get this right."""
    from litestar_queues.consumer import consume_one
    from litestar_queues.execution.cloudtasks import routes

    # Read off the module rather than the import, because the binding in that
    # module is what the route actually calls.
    routed = vars(routes)["consume_one"]
    assert routed is consume_one
    signature = consume_one.__code__.co_varnames[: consume_one.__code__.co_argcount]
    assert signature == ("queue", "task_id")


def test_only_a_self_scheduling_transport_claims_to_schedule_on_enqueue() -> "None":
    """A broker that answered true here would need a scheduler it does not have."""
    from litestar_queues.execution import list_execution_backends
    from litestar_queues.execution.base import BaseExecutionBackend
    from litestar_queues.execution.factory import get_execution_backend_class

    assert BaseExecutionBackend.schedules_on_enqueue.fget(BaseExecutionBackend()) is False  # type: ignore[attr-defined]
    self_scheduling = {
        name
        for name in list_execution_backends()
        if get_execution_backend_class(name).schedules_on_enqueue.fget(None)  # type: ignore[attr-defined]
    }
    assert self_scheduling == {"cloudtasks"}


def test_every_transport_backend_reports_itself_as_external() -> "None":
    """A transport that ran the task in-process would not need a consumer at all."""
    from litestar_queues.execution.cloudrun import CloudRunExecutionBackend
    from litestar_queues.execution.cloudtasks import CloudTasksExecutionBackend

    backends: "tuple[type[BaseExecutionBackend], ...]" = (CloudRunExecutionBackend, CloudTasksExecutionBackend)
    for backend_class in backends:
        assert backend_class.is_external.fget(None) is True  # type: ignore[attr-defined]


def test_the_public_guide_states_the_id_only_contract() -> "None":
    """An adopter who thinks arguments travel will size the wrong things."""
    guide = (DOCS / "usage/deployment/cloud-tasks.rst").read_text(encoding="utf-8")

    assert "the record's id" in guide
    assert "never cross the network" in guide


def test_no_transport_lease_is_documented_as_the_queue_heartbeat() -> "None":
    """Broker redelivery clocks expire on their own schedule and fence nothing.

    Mapping one onto the queue's heartbeat is how a long task gets re-executed in
    a loop, so no public document may present them as the same mechanism.
    """
    for path, text in _published_text():
        lowered = text.lower()
        for clock in ("visibility timeout", "ack deadline", "consumer offset"):
            if clock in lowered:
                assert "heartbeat lease" not in lowered, f"{path} ties {clock} to the queue heartbeat"

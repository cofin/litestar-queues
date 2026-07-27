"""Proof that the scale-to-zero topology actually works, run by hand.

Google documents no local Cloud Tasks API, so every guarantee this backend makes
about a real deployment -- a cold instance waking up, a delayed delivery being
held for a day and not a millisecond less, a redelivery finding the record
already owned -- is unfalsifiable in CI. These cases are the falsification, and
they run only when an operator points them at their own project and says so.

They are release evidence, not a gate. Everything CI can prove is proved by the
unit tier against an injected client; what is left here is the part that needs
Google, a deployed private service, and a database both processes can reach.

Read ``README.md`` in this directory before running them.
"""

import asyncio
import os
import platform
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import pytest

from litestar_queues.consumer import _import_factory
from litestar_queues.execution.cloudtasks.backend import _create_task_request, _is_already_exists
from tests.integration.execution.cloudtasks.live import (
    CONFIG_FACTORY_ENV,
    EVIDENCE_PATH_ENV,
    DeliveryJanitor,
    Evidence,
    live_skip_reason,
    live_timeout,
)
from tests.integration.execution.cloudtasks.probe_tasks import FAILS_ALWAYS, SUCCEEDS

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

    from litestar_queues import QueueService
    from litestar_queues.execution.cloudtasks import CloudTasksExecutionBackend
    from litestar_queues.models import QueuedTaskRecord

_SKIP_REASON = live_skip_reason(os.environ)
pytestmark = [pytest.mark.anyio, pytest.mark.skipif(_SKIP_REASON is not None, reason=_SKIP_REASON or "")]

POLL_INTERVAL = 1.0
DELAYED_BY = 45.0


class Topology:
    """One live queue, its transport client, and the janitor watching both."""

    __slots__ = ("backend", "client", "evidence", "janitor", "service", "timeout")

    def __init__(
        self,
        service: "QueueService",
        backend: "CloudTasksExecutionBackend",
        client: "Any",
        janitor: "DeliveryJanitor",
        evidence: "Evidence",
        timeout: "float",
    ) -> "None":
        self.service = service
        self.backend = backend
        self.client = client
        self.janitor = janitor
        self.evidence = evidence
        self.timeout = timeout

    async def enqueue(self, task_name: "str", **kwargs: "Any") -> "QueuedTaskRecord":
        """Enqueue one probe and register its delivery for cleanup.

        Returns:
            The persisted record, delivery reference included.

        Raises:
            AssertionError: If the record did not reach storage.
        """
        result = await self.service.enqueue(task_name, **kwargs)
        record = await self.service.get_task(result.id)
        assert record is not None, "enqueue returned an id that storage does not hold"
        if record.execution_ref:
            self.janitor.record(record.execution_ref)
        return record

    async def settle(self, record_id: "Any", *, within: "float | None" = None) -> "QueuedTaskRecord":
        """Wait for one record to reach a terminal status.

        Returns:
            The terminal record.

        Raises:
            AssertionError: If the record did not settle inside the budget.
        """
        deadline = time.monotonic() + (within if within is not None else self.timeout)
        latest: "QueuedTaskRecord | None" = None
        while time.monotonic() < deadline:
            latest = await self.service.get_task(record_id)
            if latest is not None and latest.is_terminal:
                return latest
            await asyncio.sleep(POLL_INTERVAL)
        status = latest.status if latest is not None else "missing"
        msg = f"record {record_id} did not settle within the budget; last status was {status!r}"
        raise AssertionError(msg)

    async def delivery_exists(self, task_name: "str") -> "bool":
        """Whether the transport still holds a named delivery.

        Returns:
            True when Cloud Tasks returns the task.
        """
        try:
            await self.client.get_task(name=task_name)
        except Exception as exc:
            if exc.__class__.__name__ == "NotFound":
                return False
            raise
        return True


@pytest.fixture(scope="session")
def evidence(tmp_path_factory: "pytest.TempPathFactory") -> "Iterator[Evidence]":
    """Collect timing evidence and write it locally at the end of the run.

    Yields:
        The evidence collector.
    """
    collected = Evidence()
    yield collected
    configured = os.environ.get(EVIDENCE_PATH_ENV)
    destination = Path(configured) if configured else tmp_path_factory.mktemp("live-evidence") / "cloud-tasks.json"
    collected.write(destination)
    print(f"\nlive Cloud Tasks evidence written to {destination}")


@pytest.fixture
async def topology(evidence: "Evidence") -> "AsyncIterator[Topology]":
    """Open the operator's configured queue against the real transport.

    The configuration is imported here rather than at module scope, because
    importing it is what discovers credentials, and that must happen only after
    the gate has authorized the run.

    Yields:
        The live topology.
    """
    from litestar_queues import QueueService
    from litestar_queues.config import QueueConfig
    from litestar_queues.execution.cloudtasks import CloudTasksExecutionBackend

    provided = _import_factory(os.environ[CONFIG_FACTORY_ENV])()
    # The consumer seam also accepts a service or a context manager; the proof
    # needs the configuration itself, so it can open its own producer against
    # the same storage rather than inheriting the consumer's lifecycle.
    assert isinstance(provided, QueueConfig), f"{CONFIG_FACTORY_ENV} must return a QueueConfig for the live proof"
    async with QueueService(provided) as service:
        backend = service.get_execution_backend()
        assert isinstance(backend, CloudTasksExecutionBackend), "the configured queue does not run on Cloud Tasks"
        client = await backend._get_client()
        async with DeliveryJanitor(client) as janitor:
            yield Topology(service, backend, client, janitor, evidence, live_timeout(os.environ))


# --------------------------------------------------------------------------- the topology


async def test_an_enqueued_record_wakes_a_cold_instance_and_comes_back_terminal(topology: "Topology") -> "None":
    """The whole proposition: no worker anywhere, and the work still gets done."""
    started = time.monotonic()

    record = await topology.enqueue(SUCCEEDS)
    settled = await topology.settle(record.id)

    assert settled.status == "completed"
    assert settled.result is not None
    topology.evidence.record(
        "cold_delivery",
        seconds=round(time.monotonic() - started, 3),
        location=topology.backend.execution_config.location,
        runtime=platform.python_version(),
        consumer_ran_at=settled.result.get("ran_at") if isinstance(settled.result, dict) else None,
    )


async def test_a_warm_instance_answers_faster_than_a_cold_one(topology: "Topology") -> "None":
    """Recorded rather than asserted: the number is the evidence, not the bound."""
    durations: "list[float]" = []
    for _attempt in range(2):
        started = time.monotonic()
        record = await topology.enqueue(SUCCEEDS)
        assert (await topology.settle(record.id)).status == "completed"
        durations.append(round(time.monotonic() - started, 3))

    topology.evidence.record(
        "cold_then_warm",
        cold_seconds=durations[0],
        warm_seconds=durations[1],
        location=topology.backend.execution_config.location,
        runtime=platform.python_version(),
    )


async def test_a_delayed_record_is_not_claimed_before_it_is_due(topology: "Topology") -> "None":
    """Cloud Tasks holds the delivery; nothing polls, so nothing can run early."""
    due = datetime.now(timezone.utc) + timedelta(seconds=DELAYED_BY)

    record = await topology.enqueue(SUCCEEDS, scheduled_at=due)
    await asyncio.sleep(DELAYED_BY / 3)
    early = await topology.service.get_task(record.id)

    assert early is not None
    assert not early.is_terminal
    assert early.status in {"pending", "scheduled"}
    settled = await topology.settle(record.id, within=DELAYED_BY + topology.timeout)
    assert settled.status == "completed"


async def test_a_redelivered_record_is_only_run_by_one_consumer(topology: "Topology") -> "None":
    """At-least-once is documented, so a second delivery has to be harmless."""
    due = datetime.now(timezone.utc) + timedelta(seconds=DELAYED_BY)
    record = await topology.enqueue(SUCCEEDS, scheduled_at=due)
    duplicate_name = f"{topology.backend.execution_config.queue_path}/tasks/lq-dup-{uuid4().hex}"
    await topology.client.create_task(
        request=_create_task_request(topology.backend.execution_config, record, duplicate_name)
    )
    topology.janitor.record(duplicate_name)

    settled = await topology.settle(record.id, within=DELAYED_BY + topology.timeout)

    assert settled.status == "completed"
    # One claim owner: a second run would have raised the attempt count.
    assert settled.retry_count == 0


async def test_a_failing_task_gets_one_more_delivery_and_then_settles(topology: "Topology") -> "None":
    """The retry delivery is created by the package, from inside the consumer."""
    record = await topology.enqueue(FAILS_ALWAYS)

    settled = await topology.settle(record.id, within=topology.timeout * 2)

    assert settled.status == "failed"
    assert settled.retry_count == 1
    if settled.execution_ref:
        topology.janitor.record(settled.execution_ref)


async def test_a_cancelled_record_is_acknowledged_without_running(topology: "Topology") -> "None":
    """Storage decides, and its decision outlives a delivery Google already holds."""
    due = datetime.now(timezone.utc) + timedelta(seconds=DELAYED_BY)
    record = await topology.enqueue(SUCCEEDS, scheduled_at=due)
    assert await topology.service.get_queue_backend().cancel_task(record.id) is not None

    await asyncio.sleep(DELAYED_BY + POLL_INTERVAL * 5)
    current = await topology.service.get_task(record.id)

    assert current is not None
    assert current.status == "cancelled"
    assert current.result is None
    assert not await topology.delivery_exists(record.execution_ref or "")


async def test_a_missing_delivery_is_recreated_by_one_bounded_repair(topology: "Topology") -> "None":
    """Nothing polls these records, so a lost delivery is a record lost forever."""
    due = datetime.now(timezone.utc) + timedelta(days=1)
    record = await topology.enqueue(SUCCEEDS, scheduled_at=due)
    original = record.execution_ref
    assert original is not None
    await topology.client.delete_task(name=original)

    outcome = await topology.backend.repair(topology.service, limit=5)

    assert outcome.changed == 1
    repaired = await topology.service.get_task(record.id)
    assert repaired is not None
    assert repaired.execution_ref != original
    topology.janitor.record(repaired.execution_ref or "")
    assert await topology.delivery_exists(repaired.execution_ref or "")


async def test_the_real_already_exists_error_is_the_one_the_package_matches(topology: "Topology") -> "None":
    """The whole ambiguous-create path hinges on recognizing this exact error.

    It is matched structurally so the package imports no Google error types, and
    a fake cannot prove that the structure it imitates is the real one.
    """
    due = datetime.now(timezone.utc) + timedelta(days=1)
    record = await topology.enqueue(SUCCEEDS, scheduled_at=due)
    name = record.execution_ref
    assert name is not None

    with pytest.raises(Exception) as caught:
        await topology.client.create_task(request=_create_task_request(topology.backend.execution_config, record, name))

    assert _is_already_exists(caught.value)

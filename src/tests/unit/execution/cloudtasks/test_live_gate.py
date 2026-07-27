"""The harness that runs the live Cloud Tasks proof, tested without running it.

The proof itself needs a Google project, a deployed private service, and a
database both processes can reach, so it cannot run here. What can run here is
everything that decides whether it runs at all -- and that is the part with teeth.

Two properties matter. The skip decision must be reached from environment
variables alone, because loading the operator's configuration is what triggers
credential discovery and a metadata-server hop; a suite that discovers
credentials in order to decide not to run has already done the thing it was
gated against. And cleanup must delete exactly the deliveries the run created,
including when the run failed, because the alternative is a suite that either
leaves paid work queued against a real service or reaches for something it did
not create.
"""

from typing import TYPE_CHECKING, Any

import pytest

from tests.integration.execution.cloudtasks.live import (
    CONFIG_FACTORY_ENV,
    LIVE_FLAG_ENV,
    DeliveryJanitor,
    live_skip_reason,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

pytestmark = pytest.mark.anyio

CONFIGURED = {LIVE_FLAG_ENV: "1", CONFIG_FACTORY_ENV: "example.app:queue_config"}


class ExplodingClient:
    """A stand-in that records deletions and can refuse any of them."""

    __slots__ = ("deleted", "missing", "unreachable")

    def __init__(self, missing: "tuple[str, ...]" = (), *, unreachable: "bool" = False) -> "None":
        self.deleted: "list[str]" = []
        self.missing = missing
        self.unreachable = unreachable

    async def delete_task(self, *, name: "str", timeout: "float | None" = None) -> "None":
        """Record a deletion.

        Raises:
            NotFound: If the named task was declared already gone.
            ConnectionError: If the API was declared unreachable.
        """
        del timeout
        self.deleted.append(name)
        if self.unreachable:
            msg = "the API did not answer"
            raise ConnectionError(msg)
        if name in self.missing:
            from tests.unit.execution.cloudtasks._fakes import NotFound

            msg = "task not found"
            raise NotFound(msg)


# --------------------------------------------------------------------------- gating


def test_the_proof_does_not_run_unless_it_is_asked_for() -> "None":
    """Default-off, because the default is somebody else's Google bill."""
    assert live_skip_reason({}) is not None
    assert live_skip_reason({CONFIG_FACTORY_ENV: "example.app:queue_config"}) is not None


def test_the_proof_does_not_run_without_a_topology_to_run_against() -> "None":
    reason = live_skip_reason({LIVE_FLAG_ENV: "1"})

    assert reason is not None
    assert CONFIG_FACTORY_ENV in reason


def test_a_named_topology_with_the_flag_set_runs() -> "None":
    assert live_skip_reason(CONFIGURED) is None


def test_deciding_not_to_run_never_loads_the_operator_configuration() -> "None":
    """Loading it is what reaches for credentials, which is the point of the gate."""
    hostile = {LIVE_FLAG_ENV: "0", CONFIG_FACTORY_ENV: "tests.unit.execution.cloudtasks.no_such_module:boom"}

    assert live_skip_reason(hostile) is not None


def test_the_flag_is_read_exactly_and_not_as_truthiness() -> "None":
    """``LIVE=0`` and ``LIVE=false`` are an operator saying no, not saying nothing."""
    for value in ("0", "false", "no", ""):
        assert live_skip_reason({**CONFIGURED, LIVE_FLAG_ENV: value}) is not None


# --------------------------------------------------------------------------- cleanup


async def test_cleanup_deletes_exactly_the_deliveries_it_created() -> "None":
    client = ExplodingClient()

    async with DeliveryJanitor(client) as janitor:
        janitor.record("projects/p/locations/l/queues/q/tasks/lq-one")
        janitor.record("projects/p/locations/l/queues/q/tasks/lq-two")

    assert client.deleted == [
        "projects/p/locations/l/queues/q/tasks/lq-one",
        "projects/p/locations/l/queues/q/tasks/lq-two",
    ]


async def test_cleanup_still_runs_when_the_proof_failed() -> "None":
    """A failed run is exactly when paid work is most likely to be left queued."""
    client = ExplodingClient()

    with pytest.raises(RuntimeError, match="the proof failed"):
        async with DeliveryJanitor(client) as janitor:
            janitor.record("projects/p/locations/l/queues/q/tasks/lq-one")
            msg = "the proof failed"
            raise RuntimeError(msg)

    assert client.deleted == ["projects/p/locations/l/queues/q/tasks/lq-one"]


async def test_cleanup_touches_nothing_it_did_not_create() -> "None":
    """The queue is the operator's, and may be holding work this run knows nothing about."""
    client = ExplodingClient()

    async with DeliveryJanitor(client):
        pass

    assert client.deleted == []


async def test_a_delivery_google_already_ran_does_not_break_cleanup() -> "None":
    """Most of these deliveries are meant to be dispatched before the run ends."""
    gone = "projects/p/locations/l/queues/q/tasks/lq-dispatched"
    client = ExplodingClient(missing=(gone,))

    async with DeliveryJanitor(client) as janitor:
        janitor.record(gone)
        janitor.record("projects/p/locations/l/queues/q/tasks/lq-still-there")

    assert client.deleted == [gone, "projects/p/locations/l/queues/q/tasks/lq-still-there"]


async def test_one_undeletable_delivery_does_not_strand_the_rest() -> "None":
    """Giving up on the first refusal would leave paid work queued behind it."""
    client = ExplodingClient(unreachable=True)
    names = [f"projects/p/locations/l/queues/q/tasks/lq-{index}" for index in range(3)]

    async with DeliveryJanitor(client) as janitor:
        for name in names:
            janitor.record(name)

    assert client.deleted == names


@pytest.fixture(autouse=True)
def _no_ambient_live_run(monkeypatch: "pytest.MonkeyPatch") -> "Iterator[None]":
    """Never read a real operator's environment while proving the gate.

    Yields:
        Control, with both gate variables removed.
    """
    monkeypatch.delenv(LIVE_FLAG_ENV, raising=False)
    monkeypatch.delenv(CONFIG_FACTORY_ENV, raising=False)
    yield


def test_the_harness_module_pulls_in_no_google_client() -> "None":
    """It is imported by this unit tier, which runs without the extra."""
    from tests.integration.execution.cloudtasks import live

    assert live.__name__
    assert not any(name.startswith("google.cloud.tasks") for name in _imported_by(live))


def _imported_by(module: "Any") -> "list[str]":
    """Names the module bound at import time.

    Returns:
        Every module name reachable as a module-level attribute.
    """
    import types

    return [value.__name__ for value in vars(module).values() if isinstance(value, types.ModuleType)]

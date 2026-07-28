"""Gating, cleanup, and evidence for the live Cloud Tasks topology proof.

Cloud Tasks has no local API emulator, so the only way to prove that a record
enqueued on one process reaches a cold private Cloud Run instance and comes back
terminal is to do it against Google. That is a paid, credentialed, side-effecting
run, which makes the interesting engineering the part that decides whether it
happens.

The skip decision is reached from environment variables and nothing else.
Resolving the operator's configuration is what triggers credential discovery and
a metadata-server hop, so it happens strictly after the gate has said yes -- a
suite that authenticates in order to decide not to run has already done the thing
the gate exists to prevent.

Nothing here provisions anything. The queue, the service, and the IAM bindings
are the operator's, pre-provisioned and named through their own configuration.
The only resources this creates are deliveries, created by the package under
test, and every one of them is recorded by the exact name Google returned and
deleted by that name in a ``finally``.
"""

import json
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Mapping
    from pathlib import Path
    from types import TracebackType

    from typing_extensions import Self

__all__ = (
    "CONFIG_FACTORY_ENV",
    "EVIDENCE_PATH_ENV",
    "LIVE_FLAG_ENV",
    "TIMEOUT_ENV",
    "DeliveryJanitor",
    "Evidence",
    "live_skip_reason",
    "live_timeout",
)

LIVE_FLAG_ENV = "LITESTAR_QUEUES_GCP_LIVE"
"""Set to exactly ``1`` to opt in. Anything else is an operator saying no."""

CONFIG_FACTORY_ENV = "QUEUES_CONFIG_FACTORY"
"""Import path to the queue configuration both processes share.

Deliberately the package's own consumer seam rather than a set of topology
variables: the proof is only worth running if the producer and the deployed
consumer resolve one configuration, and a queue backend is a configured object
with a pool and a schema, not a string that fits in an environment variable.
"""

EVIDENCE_PATH_ENV = "LITESTAR_QUEUES_GCP_EVIDENCE_PATH"
"""Where to write the run's timing evidence; unset writes under pytest's tmp dir."""

TIMEOUT_ENV = "LITESTAR_QUEUES_GCP_TIMEOUT"
"""Seconds to wait for a record to settle, generous enough for a cold start."""

DEFAULT_TIMEOUT = 180.0

logger = logging.getLogger(__name__)


def live_skip_reason(env: "Mapping[str, str]") -> "str | None":
    """Decide whether the live proof may run, reading only the environment.

    Returns:
        A reason to skip, or ``None`` when the run is authorized.
    """
    if env.get(LIVE_FLAG_ENV) != "1":
        return f"live GCP proof not requested: set {LIVE_FLAG_ENV}=1 to run it"
    if not env.get(CONFIG_FACTORY_ENV):
        return (
            f"live GCP proof requested but no topology named: set {CONFIG_FACTORY_ENV} to the "
            f"import path of the queue configuration the deployed consumer also uses"
        )
    return None


def live_timeout(env: "Mapping[str, str]") -> "float":
    """How long a record may take to settle, cold start included.

    Returns:
        The configured timeout in seconds.
    """
    raw = env.get(TIMEOUT_ENV)
    return float(raw) if raw else DEFAULT_TIMEOUT


class DeliveryJanitor:
    """Deletes exactly the deliveries a run created, and nothing else.

    The queue belongs to the operator and may be holding work this run knows
    nothing about, so there is no listing and no prefix sweep: a name is deleted
    only because this run watched the package create it.

    Cleanup is best-effort by design. Most of these deliveries are meant to be
    dispatched before the run ends, so finding one already gone is the expected
    case, and one unreachable delete must not strand the rest.
    """

    __slots__ = ("_client", "_created")

    def __init__(self, client: "Any") -> "None":
        self._client = client
        self._created: "list[str]" = []

    def record(self, task_name: "str") -> "None":
        """Remember one delivery by the exact resource name Google returned."""
        self._created.append(task_name)

    @property
    def created(self) -> "tuple[str, ...]":
        """Every delivery name this run recorded."""
        return tuple(self._created)

    async def __aenter__(self) -> "Self":
        return self

    async def __aexit__(
        self,
        exc_type: "type[BaseException] | None",  # noqa: PYI036
        exc_val: "BaseException | None",  # noqa: PYI036
        exc_tb: "TracebackType | None",  # noqa: PYI036
    ) -> "None":
        del exc_type, exc_val, exc_tb
        for name in self._created:
            await self._delete(name)

    async def _delete(self, name: "str") -> "None":
        """Delete one recorded delivery, surviving whatever the API says about it."""
        try:
            await self._client.delete_task(name=name)
        except Exception:  # noqa: BLE001 - one stuck delete must not strand the others.
            logger.warning("Live proof could not delete a delivery it created: %s", name)


@dataclass(slots=True)
class Evidence:
    """Timing and outcome evidence, written locally and nowhere else."""

    entries: "list[dict[str, Any]]" = field(default_factory=list)

    def record(self, case: "str", **fields: "Any") -> "None":
        """Add one case's evidence."""
        self.entries.append({"case": case, **fields})

    def write(self, path: "Path") -> "None":
        """Write the collected evidence as JSON."""
        path.write_text(json.dumps(self.entries, indent=2, default=str), encoding="utf-8")

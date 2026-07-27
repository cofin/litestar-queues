"""The tasks the deployed consumer must know about for the live proof.

This module is the contract between the runbook and the suite: the proof
enqueues these names, and a private Cloud Run service that has not registered
them will fail every record as an unknown task instead of running it. Copy this
file into the consumer image and point ``LITESTAR_QUEUES_TASK_MODULES`` at it, so
there is one definition of the names rather than two that can drift.

Nothing here does real work. A probe that took a meaningful amount of time would
measure the probe rather than the topology, and the topology is the thing under
test.
"""

from datetime import datetime, timezone

from litestar_queues import task

__all__ = ("FAILS_ALWAYS", "SUCCEEDS", "fails_always", "succeeds")

SUCCEEDS = "litestar_queues.live.succeeds"
FAILS_ALWAYS = "litestar_queues.live.fails_always"


@task(SUCCEEDS)
async def succeeds() -> "dict[str, str]":
    """Complete immediately, stamping when and where the consumer ran it.

    Returns:
        The consumer-side completion time, which is the evidence that a cold
        instance woke up and did the work.
    """
    return {"ran_at": datetime.now(timezone.utc).isoformat()}


@task(FAILS_ALWAYS, retries=1)
async def fails_always() -> "None":
    """Fail every attempt, so one retry is created and then the record settles.

    Raises:
        RuntimeError: Always.
    """
    msg = "live proof probe failure"
    raise RuntimeError(msg)

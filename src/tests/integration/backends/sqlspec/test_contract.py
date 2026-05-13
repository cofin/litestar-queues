"""Generic queue-backend contract tests parametrized across the registry."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from litestar_queues.backends import BaseQueueBackend

pytestmark = pytest.mark.anyio


async def test_backend_contract_enqueue_claim_complete_cycle(queue_backend: "BaseQueueBackend") -> None:
    """A backend must support the full enqueue → claim → complete cycle."""
    record = await queue_backend.enqueue("tasks.contract.cycle", priority=10)

    claimed = await queue_backend.claim_task(record.id)
    assert claimed is not None
    assert claimed.id == record.id
    assert claimed.status == "running"

    await queue_backend.complete_task(claimed.id, result={"ok": True})

    stored = await queue_backend.get_task(record.id)
    assert stored is not None
    assert stored.status == "completed"

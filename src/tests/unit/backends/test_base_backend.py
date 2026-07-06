from uuid import uuid4

import pytest

from litestar_queues import HeartbeatTouch, HeartbeatTouchResult
from litestar_queues.backends import BaseQueueBackend

pytestmark = pytest.mark.anyio


async def test_base_backend_touch_heartbeats_empty_input_is_idempotent() -> "None":
    result = await BaseQueueBackend().touch_heartbeats([])

    assert result == HeartbeatTouchResult()


async def test_base_backend_touch_heartbeats_misses_unknown_tasks() -> "None":
    task_id = uuid4()

    result = await BaseQueueBackend().touch_heartbeats([HeartbeatTouch(task_id=task_id, expected_retry_count=None)])

    assert result.touched_task_ids == set()
    assert result.missed_task_ids == {task_id}
    assert result.failed_task_ids == set()


def test_base_backend_has_no_single_task_heartbeat_api() -> "None":
    single_task_heartbeat_api = "touch" + "_heartbeat"

    assert not hasattr(BaseQueueBackend, single_task_heartbeat_api)

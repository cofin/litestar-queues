"""Behavioral shutdown-requeue proof for every registered queue backend."""

from typing import TYPE_CHECKING

import pytest

from tests.integration._interrupt_contract import assert_worker_shutdown_requeues_running_task

if TYPE_CHECKING:
    from litestar_queues.backends import BaseQueueBackend

pytestmark = pytest.mark.anyio


async def test_worker_shutdown_requeues_running_task(queue_backend: "BaseQueueBackend") -> "None":
    await assert_worker_shutdown_requeues_running_task(queue_backend)

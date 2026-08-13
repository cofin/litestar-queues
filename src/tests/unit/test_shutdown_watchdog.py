"""Hard-exit watchdog behavior for workers whose loop or task will not die."""

import signal
import time

import pytest

from litestar_queues._cli import _CLIStopCoordinator
from litestar_queues.worker.runtime import ShutdownWatchdog

pytestmark = pytest.mark.anyio


def test_watchdog_exits_after_its_deadline() -> "None":
    exits: "list[int]" = []
    watchdog = ShutdownWatchdog(timeout=0.05, exit_fn=exits.append)

    watchdog.arm(signal.SIGTERM)
    time.sleep(0.4)

    assert exits == [128 + int(signal.SIGTERM)]


def test_watchdog_disarm_prevents_the_exit() -> "None":
    exits: "list[int]" = []
    watchdog = ShutdownWatchdog(timeout=0.2, exit_fn=exits.append)

    watchdog.arm(signal.SIGINT)
    watchdog.disarm()
    time.sleep(0.4)

    assert exits == []


def test_watchdog_without_a_timeout_never_arms() -> "None":
    exits: "list[int]" = []
    watchdog = ShutdownWatchdog(timeout=None, exit_fn=exits.append)

    watchdog.arm(signal.SIGTERM)
    time.sleep(0.2)

    assert exits == []


def test_watchdog_arm_is_idempotent_and_keeps_the_first_signal() -> "None":
    exits: "list[int]" = []
    watchdog = ShutdownWatchdog(timeout=0.05, exit_fn=exits.append)

    watchdog.arm(signal.SIGINT)
    watchdog.arm(signal.SIGTERM)
    time.sleep(0.4)

    assert exits == [128 + int(signal.SIGINT)]


async def test_second_signal_arms_the_watchdog_and_the_third_exits_immediately() -> "None":
    exits: "list[int]" = []
    watchdog = ShutdownWatchdog(timeout=30.0, exit_fn=exits.append)
    coordinator = _CLIStopCoordinator(watchdog=watchdog)

    coordinator.request_stop(signal.SIGTERM)
    assert coordinator.graceful.is_set()
    assert not coordinator.force.is_set()
    assert exits == []

    coordinator.request_stop(signal.SIGTERM)
    assert coordinator.force.is_set()
    assert watchdog.is_armed
    assert exits == []

    coordinator.request_stop(signal.SIGTERM)
    assert exits == [128 + int(signal.SIGTERM)]

    watchdog.disarm()

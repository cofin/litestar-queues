"""Tests for the ``litestar queues`` CLI subcommand group.

The ``run`` subcommand is exercised by ``test_cli_run.py`` via subprocess
because ``CliRunner`` cannot deliver real signals.
"""

import json
import sys
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Iterator

    from click.testing import Result

pytestmark = pytest.mark.anyio


@pytest.fixture(autouse=True)
def _evict_support_modules() -> "Iterator[None]":
    """Force re-import of tests.support.* between tests so task_modules re-register cleanly.

    ``clean_task_registry`` (autouse, project conftest) clears the registry but
    ``load_task_modules`` short-circuits on ``_loaded_modules``; evicting
    forces re-import so the canary task is registered for each invocation.

    Yields:
        Control to the test before clearing cached support modules.
    """
    yield
    for name in list(sys.modules):
        if name == "tests._factories.queue_tasks":
            del sys.modules[name]
    from litestar_queues.task import _loaded_modules

    _loaded_modules.discard("tests._factories.queue_tasks")


def _runner_invoke(app_target: "str", args: "list[str]", monkeypatch: "pytest.MonkeyPatch") -> "Result":
    """Invoke ``litestar`` CLI with the configured app target via ``CliRunner``.

    Returns:
        The ``Result`` object so callers can assert on ``exit_code``, ``stdout``, and ``stderr``.
    """
    from click.testing import CliRunner
    from litestar.cli.main import litestar_group

    monkeypatch.setenv("LITESTAR_APP", app_target)
    runner = CliRunner()
    return runner.invoke(litestar_group, args, catch_exceptions=False)


def test_litestar_queues_help_lists_subcommands(monkeypatch: "pytest.MonkeyPatch") -> "None":
    result = _runner_invoke("tests.support.cli_app:app", ["queues", "--help"], monkeypatch)

    assert result.exit_code == 0, result.stderr
    assert "run" in result.stdout
    assert "status" in result.stdout
    assert "scheduler-health" in result.stdout


def test_cli_module_exposes_public_command_callbacks() -> "None":
    from litestar_queues import _cli

    expected_callbacks = {
        "run": "run_command",
        "scheduler-health": "scheduler_health_command",
        "status": "status_command",
    }

    for command_name, callback_name in expected_callbacks.items():
        command = _cli.queues_group.commands[command_name]
        assert command.callback is not None
        assert command.callback.__name__ == callback_name
        assert hasattr(_cli, callback_name)
        assert not hasattr(_cli, f"_{callback_name}")


def test_status_subcommand_default_table(monkeypatch: "pytest.MonkeyPatch") -> "None":
    result = _runner_invoke("tests.support.cli_app:app", ["queues", "status"], monkeypatch)

    assert result.exit_code == 0, result.stderr
    assert "pending" in result.stdout
    assert "total" in result.stdout


def test_status_subcommand_json(monkeypatch: "pytest.MonkeyPatch") -> "None":
    result = _runner_invoke("tests.support.cli_app:app", ["queues", "status", "--json"], monkeypatch)

    assert result.exit_code == 0, result.stderr
    payload = json.loads(result.stdout)
    expected_keys = {"pending", "scheduled", "running", "completed", "failed", "cancelled", "total"}
    assert expected_keys.issubset(payload.keys())
    assert all(isinstance(payload[key], int) for key in expected_keys)


def test_status_subcommand_advisory_queue_filter(monkeypatch: "pytest.MonkeyPatch") -> "None":
    result = _runner_invoke("tests.support.cli_app:app", ["queues", "status", "--queue", "billing"], monkeypatch)

    assert result.exit_code == 0, result.stderr
    assert "advisory" in result.stderr.lower()


def test_scheduler_health_returns_4_when_no_canary_runs(monkeypatch: "pytest.MonkeyPatch") -> "None":
    """Canary task is registered but never executed, so the health check is stale."""
    result = _runner_invoke("tests.support.cli_app:app", ["queues", "scheduler-health", "--minutes", "5"], monkeypatch)

    assert result.exit_code == 4
    assert "stale" in result.stderr.lower()


def test_scheduler_health_returns_3_when_canary_not_registered(monkeypatch: "pytest.MonkeyPatch") -> "None":
    result = _runner_invoke("tests.support.cli_app_missing_canary:app", ["queues", "scheduler-health"], monkeypatch)

    assert result.exit_code == 3
    assert "canary" in result.stderr.lower()


def test_scheduler_health_returns_0_when_canary_completed_recently(monkeypatch: "pytest.MonkeyPatch") -> "None":
    """Seed a completed canary record and assert exit 0 with healthy message."""
    import anyio

    async def _seed_canary() -> "None":
        import importlib

        cli_app = importlib.import_module("tests.support.cli_app")
        from litestar_queues import QueuePlugin
        from litestar_queues.task import get_task_registry, load_task_modules

        plugin = next(p for p in cli_app.app.plugins if isinstance(p, QueuePlugin))
        config = plugin.config

        load_task_modules(config.task_modules)
        service = plugin.get_service()
        await service.open()
        try:
            canary_task = get_task_registry()[config.scheduler_canary_task]
            record = await service.enqueue(canary_task)
            await service.get_queue_backend().complete_task(record.id, result=None)
        finally:
            await service.close()

    anyio.run(_seed_canary)

    result = _runner_invoke("tests.support.cli_app:app", ["queues", "scheduler-health"], monkeypatch)

    assert result.exit_code == 0, result.stderr
    assert "healthy" in result.stdout.lower()

"""Click command surfaces for ``litestar queues …``.

This module is private. :meth:`QueuePlugin.on_cli_init` imports it lazily
so ``import litestar_queues`` does not pull ``click`` into ``sys.modules``.
Once *this* module is imported, ``import click`` at top level is fine
because the decorator-style command bodies need it at definition time.
"""

import asyncio
import contextlib
import json
import os
import signal
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, cast

import click

from litestar_queues.config import queue_backend_name
from litestar_queues.consumer import run_task
from litestar_queues.maintenance import QueueMaintenanceService
from litestar_queues.plugin import QueuePlugin
from litestar_queues.task import get_task_registry, load_task_modules
from litestar_queues.worker import Worker

if TYPE_CHECKING:
    from litestar.cli._utils import LitestarEnv

    from litestar_queues.maintenance import MaintenancePhase, QueueMaintenanceSummary
    from litestar_queues.service import QueueService

__all__ = (
    "queues_group",
    "register",
    "run_command",
    "run_maintenance_command",
    "run_task_command",
    "scheduler_health_command",
    "status_command",
)

FORCE_STOP_SIGNAL_COUNT = 2


@click.group(name="queues", help="litestar-queues operations.")
def queues_group() -> "None":
    pass


@queues_group.command(name="run", help="Start a standalone worker fleet.")
@click.option("--queue", "queues", multiple=True, help="Queue name to process. Repeatable.")
@click.option(
    "--max-concurrency",
    type=click.IntRange(min=1),
    default=None,
    help="Override WorkerConfig.max_concurrency for this run.",
)
@click.option(
    "--drain-timeout",
    type=click.FloatRange(min=0),
    default=None,
    help="Seconds to wait for in-flight tasks to drain after SIGTERM/SIGINT. "
    "Defaults to QueueConfig.worker.graceful_shutdown_timeout.",
)
def run_command(
    ctx: "click.Context", queues: "tuple[str, ...]", max_concurrency: "int | None", drain_timeout: "float | None"
) -> "None":
    env = _ensure_env(ctx)
    plugin = _resolve_plugin(env)
    config = plugin.config
    if config.task_modules:
        load_task_modules(config.task_modules)

    effective_concurrency = max_concurrency or config.worker.max_concurrency
    effective_drain_timeout = drain_timeout if drain_timeout is not None else config.worker.graceful_shutdown_timeout

    effective_queues = queues or config.worker.queues

    exit_code = asyncio.run(_run_worker(plugin, effective_concurrency, effective_drain_timeout, effective_queues))
    ctx.exit(exit_code)


@queues_group.command(name="status", help="Show queue status counts.")
@click.option(
    "--queue",
    "queue_filter",
    default=None,
    help="Filter by queue name. Currently advisory; backend filtering is not yet enforced.",
)
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable JSON.")
def status_command(ctx: "click.Context", queue_filter: "str | None", as_json: "bool") -> "None":
    env = _ensure_env(ctx)
    plugin = _resolve_plugin(env)
    exit_code = asyncio.run(_status_run(plugin, queue_filter, as_json))
    ctx.exit(exit_code)


@queues_group.command(
    name="scheduler-health", help="Exit non-zero if the scheduler canary task has not completed within the window."
)
@click.option("--minutes", type=click.IntRange(min=1), default=5, help="Staleness threshold in minutes (default 5).")
def scheduler_health_command(ctx: "click.Context", minutes: "int") -> "None":
    env = _ensure_env(ctx)
    plugin = _resolve_plugin(env)
    exit_code = asyncio.run(_scheduler_health_run(plugin, minutes))
    ctx.exit(exit_code)


@queues_group.command(
    name="run-task",
    help="Run one queued record by id (external-executor consumer). By default reads the task id "
    "(LITESTAR_QUEUES_TASK_ID) and LITESTAR_QUEUES_CONFIG_FACTORY from the environment; the options below "
    "override those defaults so a job can be run by hand.",
)
@click.option("--task-id", default=None, help="Run the queued record with this id (local one-shot).")
@click.option("--config-factory", default=None, help="``module:callable`` returning a QueueConfig or QueueService.")
@click.option("--task-modules", default=None, help="Comma-separated modules to import before running the task.")
def run_task_command(
    ctx: "click.Context", task_id: "str | None", config_factory: "str | None", task_modules: "str | None"
) -> "None":
    exit_code = asyncio.run(
        run_task(task_id=task_id, config_factory=config_factory, task_modules=task_modules, env=os.environ)
    )
    ctx.exit(int(exit_code))


@queues_group.command(
    name="run-maintenance",
    help="Run one bounded maintenance pass (external reconcile, stale recovery, and retention) and exit. "
    "Thresholds and limits come from QueueConfig.maintenance; this command never starts a worker or runs due work.",
)
@click.option(
    "--phase",
    "phases",
    multiple=True,
    type=click.Choice(["external", "stale", "terminal", "events"]),
    help="Maintenance phase to run. Repeatable; defaults to every configured phase. Only narrows configuration.",
)
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable JSON.")
def run_maintenance_command(ctx: "click.Context", phases: "tuple[str, ...]", as_json: "bool") -> "None":
    env = _ensure_env(ctx)
    plugin = _resolve_plugin(env)
    exit_code = asyncio.run(_maintain_run(plugin, phases, as_json))
    ctx.exit(exit_code)


def register(cli: "click.Group") -> "None":
    """Attach the ``queues`` subcommand group to ``cli`` (idempotent)."""
    if queues_group.name not in cli.commands:
        cli.add_command(queues_group)


async def _maintain_run(plugin: "QueuePlugin", phases: "tuple[str, ...]", as_json: "bool") -> "int":
    config = plugin.config
    maintenance_config = config.maintenance
    if maintenance_config is None:
        click.echo(
            "error: QueueConfig.maintenance is not configured; set "
            "QueueConfig(maintenance=QueueMaintenanceConfig(...)) to enable 'litestar queues run-maintenance'.",
            err=True,
        )
        return 1
    if queue_backend_name(config.queue_backend) == "memory":
        click.echo(
            "error: the in-memory queue backend is process-local and cannot be maintained from a separate "
            "CLI process; run maintenance against a persistent backend (Redis/Valkey, SQLSpec, or Advanced Alchemy).",
            err=True,
        )
        return 1
    if config.task_modules:
        load_task_modules(config.task_modules)

    service = _open_service(plugin)
    try:
        await service.open()
    except Exception as exc:
        _emit_maintenance_lifecycle_error("open", exc)
        await _close_maintenance_service(service)
        return 1

    selected = cast("tuple[MaintenancePhase, ...] | None", tuple(phases) or None)
    summary: "QueueMaintenanceSummary | None" = None
    run_failed = False
    try:
        backend = service.get_queue_backend()
        if not backend.capabilities.supports_maintenance:
            click.echo(
                f"error: {type(backend).__name__} does not support distributed maintenance coordination required by "
                "'litestar queues run-maintenance'.",
                err=True,
            )
            return 1
        summary = await QueueMaintenanceService(service, maintenance_config).run(selected)
    except Exception as exc:
        _emit_maintenance_lifecycle_error("run", exc)
        run_failed = True
    finally:
        closed = await _close_maintenance_service(service)

    if run_failed or not closed or summary is None:
        return 1
    _emit_maintenance_summary(summary, as_json)
    return _maintenance_exit_code(summary)


def _emit_maintenance_lifecycle_error(stage: "str", exc: "Exception") -> "None":
    click.echo(
        f"error: maintenance_{stage}_failed:{type(exc).__name__}. "
        "Apply the current queue backend migrations and verify backend connectivity; "
        "the underlying exception message was suppressed because it may contain credentials.",
        err=True,
    )


async def _close_maintenance_service(service: "QueueService") -> "bool":
    try:
        await service.close()
    except Exception as exc:
        _emit_maintenance_lifecycle_error("close", exc)
        return False
    return True


def _emit_maintenance_summary(summary: "QueueMaintenanceSummary", as_json: "bool") -> "None":
    if as_json:
        click.echo(json.dumps(summary.to_payload(), separators=(",", ":")))
        return
    click.echo(f"outcome: {summary.outcome}")
    click.echo(f"acquired: {summary.acquired}")
    click.echo(f"duration_ms: {summary.duration_ms:.1f}")
    click.echo(f"{'Phase':<10}{'Status':<12}{'Changed':>9}{'Duration(ms)':>14}  Error")
    click.echo(f"{'-' * 10}{'-' * 12}{'-' * 9:>9}{'-' * 12:>14}  {'-' * 5}")
    for phase in summary.phases:
        click.echo(
            f"{phase.phase:<10}{phase.status:<12}{phase.changed:>9}{phase.duration_ms:>14.1f}  {phase.error or '-'}"
        )


def _maintenance_exit_code(summary: "QueueMaintenanceSummary") -> "int":
    if summary.outcome == "failed":
        return 1
    if summary.outcome == "partial":
        return 2
    return 0


async def _run_worker(
    plugin: "QueuePlugin", max_concurrency: "int", drain_timeout: "float", queues: "tuple[str, ...]" = ()
) -> "int":
    config = plugin.config
    service = _open_service(plugin)
    await service.open()
    worker_config = replace(
        config.worker, max_concurrency=max_concurrency, graceful_shutdown_timeout=drain_timeout, queues=queues
    )
    worker = Worker(service, worker_config)

    loop = asyncio.get_running_loop()
    stop_coordinator = _WorkerStopCoordinator(worker)

    def _register_signal_handler(sig: "signal.Signals") -> "None":
        try:
            loop.add_signal_handler(sig, stop_coordinator.request_stop)
        except NotImplementedError:
            signal.signal(sig, lambda *_: stop_coordinator.request_stop())

    for sig in (signal.SIGTERM, signal.SIGINT):
        _register_signal_handler(sig)

    worker_task = asyncio.create_task(worker.start())
    await asyncio.sleep(0)
    click.echo("litestar queues worker started", err=True)
    exit_code = 0
    try:
        try:
            await asyncio.wait_for(worker_task, timeout=None)
            await stop_coordinator.wait()
            if stop_coordinator.forced_stop or stop_coordinator.drain_escalated:
                exit_code = 2
        except asyncio.CancelledError:
            with contextlib.suppress(BaseException):
                await asyncio.wait_for(worker_task, timeout=config.worker.final_cancel_timeout)
            exit_code = 2
        except Exception:
            exit_code = 1
    finally:
        with contextlib.suppress(Exception):
            await stop_coordinator.finish(timeout=drain_timeout + config.worker.final_cancel_timeout)
            await asyncio.wait_for(worker.stop(), timeout=drain_timeout)
        await service.close()
    return exit_code


class _WorkerStopCoordinator:
    __slots__ = ("drain_escalated", "forced_stop", "stop_count", "stop_task", "worker")

    def __init__(self, worker: "Worker") -> "None":
        self.worker = worker
        self.stop_count = 0
        self.stop_task: "asyncio.Task[None] | None" = None
        self.forced_stop = False
        self.drain_escalated = False

    def request_stop(self) -> "None":
        self.stop_count += 1
        if self.stop_count >= FORCE_STOP_SIGNAL_COUNT:
            self.forced_stop = True
            self._schedule_stop(force=True)
            return
        self._schedule_stop()

    async def wait(self) -> "None":
        if self.stop_task is not None:
            await self.stop_task

    async def finish(self, *, timeout: "float") -> "None":
        if self.stop_task is not None and not self.stop_task.done():
            await asyncio.wait_for(self.stop_task, timeout=timeout)

    def _schedule_stop(self, *, force: "bool" = False) -> "None":
        if self.stop_task is None or self.stop_task.done():
            self.stop_task = asyncio.create_task(self._stop_worker(force=force))

    async def _stop_worker(self, *, force: "bool" = False) -> "None":
        escalated = await self.worker.stop(force=force)
        if escalated:
            self.drain_escalated = True


async def _status_run(plugin: "QueuePlugin", queue_filter: "str | None", as_json: "bool") -> "int":
    if queue_filter is not None:
        click.echo(f"--queue is advisory; backend filtering not yet enforced (selected: {queue_filter})", err=True)

    service = _open_service(plugin)
    await service.open()
    try:
        stats = await service.get_queue_backend().get_statistics()
    except Exception as exc:
        click.echo(f"error: {exc}", err=True)
        await service.close()
        return 1
    await service.close()

    payload: "dict[str, int]" = {
        "pending": stats.pending,
        "scheduled": stats.scheduled,
        "running": stats.running,
        "completed": stats.completed,
        "failed": stats.failed,
        "cancelled": stats.cancelled,
        "total": stats.total,
    }

    if as_json:
        click.echo(json.dumps(payload, separators=(",", ":")))
    else:
        click.echo(f"{'Status':<12}{'Count':>8}")
        click.echo(f"{'-' * 12}{'-' * 8:>8}")
        for key in ("pending", "scheduled", "running", "completed", "failed", "cancelled"):
            click.echo(f"{key:<12}{payload[key]:>8}")
        click.echo(f"{'total':<12}{payload['total']:>8}")
    return 0


async def _scheduler_health_run(plugin: "QueuePlugin", minutes: "int") -> "int":
    config = plugin.config
    canary = config.scheduler_canary_task
    if config.task_modules:
        load_task_modules(config.task_modules)
    if canary not in get_task_registry():
        click.echo(
            f"canary task {canary!r} not configured; register a recurring task with "
            "this name to enable scheduler-health monitoring.",
            err=True,
        )
        return 3

    since = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    service = _open_service(plugin)
    await service.open()
    try:
        records = await service.get_queue_backend().list_completed_by_task(canary, since=since, limit=1)
    finally:
        await service.close()

    if records:
        click.echo(f"healthy: {canary} completed {records[0].completed_at!s}")
        return 0
    click.echo(f"stale: no {canary} completion within {minutes}m window since {since.isoformat()}", err=True)
    return 4


def _ensure_env(ctx: "click.Context") -> "LitestarEnv":
    from litestar.cli._utils import LitestarEnv

    if not isinstance(ctx.obj, LitestarEnv):
        ctx.obj = ctx.obj()
    return ctx.ensure_object(LitestarEnv)


def _resolve_plugin(env: "LitestarEnv") -> "QueuePlugin":
    for plugin in env.app.plugins:
        if isinstance(plugin, QueuePlugin):
            return plugin
    msg = "litestar-queues plugin not found on the loaded Litestar app."
    raise RuntimeError(msg)


def _open_service(plugin: "QueuePlugin") -> "QueueService":
    """Return a ``QueueService`` reusing the plugin's cached backend.

    CLI subcommands run outside Litestar's lifespan, so the plugin's
    ``_on_startup`` has not opened a service. We piggy-back on
    ``plugin.get_service`` which constructs one bound to the plugin's
    cached backend instance; that matters for the in-memory backend
    (state lives on the backend) and also avoids opening a second
    pool for Redis/SQLSpec-style backends.
    """
    return plugin.get_service()

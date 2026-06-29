"""Click command surfaces for ``litestar queues …``.

This module is private. :meth:`QueuePlugin.on_cli_init` imports it lazily
so ``import litestar_queues`` does not pull ``click`` into ``sys.modules``.
Once *this* module is imported, ``import click`` at top level is fine
because the decorator-style command bodies need it at definition time.
"""

import asyncio
import contextlib
import json
import signal
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

import click
from litestar.cli._utils import LitestarEnv

from litestar_queues.plugin import QueuePlugin
from litestar_queues.task import get_task_registry, load_task_modules
from litestar_queues.worker import Worker

if TYPE_CHECKING:
    from litestar_queues.service import QueueService

__all__ = ("queues_group", "register", "run_command", "scheduler_health_command", "status_command")

FORCE_STOP_SIGNAL_COUNT = 2


@click.group(name="queues", help="litestar-queues operations.")
def queues_group() -> "None":
    pass


@queues_group.command(name="run", help="Start a standalone worker fleet.")
@click.option("--queue", "queues", multiple=True, help="Queue name to process. Repeatable.")
@click.option(
    "--max-concurrency", type=click.IntRange(min=1), default=None, help="Override worker_max_concurrency for this run."
)
@click.option(
    "--drain-timeout",
    type=click.FloatRange(min=0),
    default=None,
    help="Seconds to wait for in-flight tasks to drain after SIGTERM/SIGINT. "
    "Defaults to QueueConfig.worker_graceful_shutdown_timeout.",
)
@click.pass_context
def run_command(
    ctx: "click.Context", queues: "tuple[str, ...]", max_concurrency: "int | None", drain_timeout: "float | None"
) -> "None":
    env = _ensure_env(ctx)
    plugin = _resolve_plugin(env)
    config = plugin.config
    if config.task_modules:
        load_task_modules(config.task_modules)

    effective_concurrency = max_concurrency or config.worker_max_concurrency
    effective_drain_timeout = drain_timeout if drain_timeout is not None else config.worker_graceful_shutdown_timeout

    effective_queues = queues or config.worker_queues

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
@click.pass_context
def status_command(ctx: "click.Context", queue_filter: "str | None", as_json: "bool") -> "None":
    env = _ensure_env(ctx)
    plugin = _resolve_plugin(env)
    exit_code = asyncio.run(_status_run(plugin, queue_filter, as_json))
    ctx.exit(exit_code)


@queues_group.command(
    name="scheduler-health", help="Exit non-zero if the scheduler canary task has not completed within the window."
)
@click.option("--minutes", type=click.IntRange(min=1), default=5, help="Staleness threshold in minutes (default 5).")
@click.pass_context
def scheduler_health_command(ctx: "click.Context", minutes: "int") -> "None":
    env = _ensure_env(ctx)
    plugin = _resolve_plugin(env)
    exit_code = asyncio.run(_scheduler_health_run(plugin, minutes))
    ctx.exit(exit_code)


def register(cli: "click.Group") -> "None":
    """Attach the ``queues`` subcommand group to ``cli``."""
    cli.add_command(queues_group)


async def _run_worker(
    plugin: "QueuePlugin", max_concurrency: "int", drain_timeout: "float", queues: "tuple[str, ...]" = ()
) -> "int":
    config = plugin.config
    service = _open_service(plugin)
    await service.open()
    worker = Worker(
        service,
        batch_size=config.worker_batch_size,
        poll_interval=config.worker_poll_interval,
        max_concurrency=max_concurrency,
        heartbeat_interval=config.worker_heartbeat_interval,
        reconcile_interval=config.worker_reconcile_interval,
        stale_after=(timedelta(seconds=config.worker_stale_after) if config.worker_stale_after is not None else None),
        stale_check_interval=config.worker_stale_check_interval,
        graceful_shutdown_timeout=drain_timeout,
        final_cancel_timeout=config.worker_final_cancel_timeout,
        queues=queues,
    )

    loop = asyncio.get_running_loop()
    stop_count = {"n": 0}
    stop_task: "asyncio.Task[None] | None" = None
    forced_stop = {"value": False}

    def _request_stop() -> "None":
        nonlocal stop_task
        stop_count["n"] += 1
        if stop_count["n"] >= FORCE_STOP_SIGNAL_COUNT:
            forced_stop["value"] = True
            if stop_task is None or stop_task.done():
                stop_task = asyncio.create_task(worker.stop(force=True))
            return
        if stop_task is None or stop_task.done():
            stop_task = asyncio.create_task(worker.stop())

    def _register_signal_handler(sig: "signal.Signals") -> "None":
        try:
            loop.add_signal_handler(sig, _request_stop)
        except NotImplementedError:
            signal.signal(sig, lambda *_: _request_stop())

    for sig in (signal.SIGTERM, signal.SIGINT):
        _register_signal_handler(sig)

    worker_task = asyncio.create_task(worker.start())
    await asyncio.sleep(0)
    click.echo("litestar queues worker started", err=True)
    exit_code = 0
    try:
        try:
            await asyncio.wait_for(worker_task, timeout=None)
            if forced_stop["value"]:
                exit_code = 2
        except asyncio.CancelledError:
            with contextlib.suppress(BaseException):
                await asyncio.wait_for(worker_task, timeout=config.worker_final_cancel_timeout)
            exit_code = 2
        except Exception:
            exit_code = 1
    finally:
        with contextlib.suppress(Exception):
            await asyncio.wait_for(worker.stop(), timeout=drain_timeout)
        await service.close()
    return exit_code


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

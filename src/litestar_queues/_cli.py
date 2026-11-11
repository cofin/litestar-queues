"""Click command surfaces for ``litestar queues …``.

This module is private. :meth:`QueuePlugin.on_cli_init` imports it lazily
so ``import litestar_queues`` does not pull ``click`` into ``sys.modules``.
Once *this* module is imported, ``import click`` at top level is fine
because the decorator-style command bodies need it at definition time.
"""

import asyncio
import contextlib
import json
import logging
import os
import signal
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from functools import partial
from typing import TYPE_CHECKING, Any, cast

import click

from litestar_queues.config import execution_backend_name, queue_backend_name
from litestar_queues.consumer import run_task
from litestar_queues.execution import get_execution_backend
from litestar_queues.execution.base import BaseConsumerExecutionBackend
from litestar_queues.maintenance import QueueMaintenanceService
from litestar_queues.plugin import QueuePlugin
from litestar_queues.task import get_task_registry, load_task_modules

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Sequence

    from litestar import Litestar
    from litestar.cli._utils import LitestarEnv

    from litestar_queues.maintenance import MaintenancePhase, QueueMaintenanceSummary
    from litestar_queues.models import QueuedTaskRecord, TaskStatus
    from litestar_queues.service import QueueService
    from litestar_queues.worker.runtime import ShutdownWatchdog

__all__ = (
    "list_command",
    "queues_group",
    "register",
    "run_command",
    "run_consumer_command",
    "run_maintenance_command",
    "run_task_command",
    "scheduler_health_command",
    "status_command",
)

logger = logging.getLogger(__name__)

FORCE_STOP_SIGNAL_COUNT = 2

_TASK_STATUS_CHOICES: "tuple[str, ...]" = (
    "pending",
    "scheduled",
    "running",
    "completed",
    "failed",
    "cancelled",
    "expired",
)

_EPHEMERAL_UNREACHABLE = (
    "queue_backend='ephemeral' lives in a private database that its owning 'litestar run' "
    "invocation creates and removes, so a separate queue command cannot attach to it. "
    "Configure a persistent backend to use 'litestar queues {command}'."
)
_MEMORY_UNREACHABLE = (
    "queue_backend='memory' keeps its records inside the process that created them, so a "
    "standalone worker would poll an empty queue forever while appearing healthy. "
    "Configure a persistent backend to use 'litestar queues run'."
)


def _reject_ephemeral_storage(plugin: "QueuePlugin", command: "str") -> "None":
    """Fail before opening a service when the database belongs to another invocation.

    Raises:
        click.ClickException: If the configured backend is server-owned ephemeral storage.
    """
    if queue_backend_name(plugin.config.queue_backend) == "ephemeral":
        raise click.ClickException(_EPHEMERAL_UNREACHABLE.format(command=command))


def _reject_inline_execution(plugin: "QueuePlugin") -> "None":
    """Fail when nothing is ever queued for a standalone worker to claim.

    Placement is deliberately not checked here. Adding standalone workers to an
    application that already runs one is how a deployment scales out, and every
    configuration where that cannot work is rejected by the storage guard above.

    Raises:
        click.ClickException: If execution is inline.
    """
    if execution_backend_name(plugin.config.execution_backend) == "immediate":
        msg = (
            "execution_backend='immediate' runs tasks inline at enqueue time, so a standalone "
            "worker would have nothing to claim. Use execution_backend='local'."
        )
        raise click.ClickException(msg)


def _reject_self_dispatching_execution(plugin: "QueuePlugin") -> "None":
    """Fail when the transport already schedules every record it is given.

    Asks the resolved backend rather than matching a name, so a future managed
    transport is covered the moment it declares the capability.

    Raises:
        click.ClickException: If the execution backend schedules on enqueue.
    """
    backend = get_execution_backend(plugin.config.execution_backend, config=plugin.config)
    if backend.schedules_on_enqueue:
        name = execution_backend_name(plugin.config.execution_backend)
        msg = (
            f"execution_backend={name!r} schedules delivery when a record is persisted, so a "
            f"standalone worker would dispatch it a second time. Run this deployment with no "
            f"worker process."
        )
        raise click.ClickException(msg)


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
    _reject_ephemeral_storage(plugin, "run")
    if queue_backend_name(config.queue_backend) == "memory":
        raise click.ClickException(_MEMORY_UNREACHABLE)
    _reject_inline_execution(plugin)
    _reject_self_dispatching_execution(plugin)

    effective_concurrency = max_concurrency or config.worker.max_concurrency
    effective_drain_timeout = drain_timeout if drain_timeout is not None else config.worker.graceful_shutdown_timeout
    effective_queues = queues or config.worker.queues

    exit_code = asyncio.run(
        _run_worker(plugin, effective_concurrency, effective_drain_timeout, effective_queues, app=env.app)
    )
    ctx.exit(exit_code)


@queues_group.command(name="run-consumer", help="Start a continuous external broker consumer.")
@click.option(
    "--backend",
    type=click.Choice(["kafka", "pubsub", "rabbitmq", "sqs"]),
    required=True,
    help="Configured broker backend to consume.",
)
@click.option("--max-concurrency", type=click.IntRange(min=1), default=None)
@click.option("--drain-timeout", type=click.FloatRange(min=0), default=None)
def run_consumer_command(
    ctx: "click.Context", backend: "str", max_concurrency: "int | None", drain_timeout: "float | None"
) -> "None":
    env = _ensure_env(ctx)
    plugin = _resolve_plugin(env)
    _reject_ephemeral_storage(plugin, "run-consumer")
    if queue_backend_name(plugin.config.queue_backend) == "memory":
        raise click.ClickException(_MEMORY_UNREACHABLE.replace("queues run", "queues run-consumer"))
    configured = execution_backend_name(plugin.config.execution_backend)
    if configured != backend:
        msg = f"--backend {backend!r} does not match configured execution backend {configured!r}."
        raise click.ClickException(msg)
    consumer = get_execution_backend(plugin.config.execution_backend, config=plugin.config)
    if not isinstance(consumer, BaseConsumerExecutionBackend):
        msg = f"execution backend {configured!r} does not support run-consumer."
        raise click.ClickException(msg)
    concurrency = max_concurrency or plugin.config.worker.max_concurrency
    timeout = drain_timeout if drain_timeout is not None else plugin.config.worker.graceful_shutdown_timeout
    ctx.exit(asyncio.run(_run_consumer(plugin, consumer, concurrency, timeout, app=env.app)))


@contextlib.asynccontextmanager
async def _enter_standalone_lifespan(
    plugin: "QueuePlugin", app: "Litestar | None", service: "QueueService"
) -> "AsyncIterator[QueueService]":
    if hasattr(plugin, "standalone_app_lifespan"):
        async with plugin.standalone_app_lifespan(app, service=service) as bound:
            yield bound
    else:
        yield service


async def _run_consumer(
    plugin: "QueuePlugin",
    backend: "BaseConsumerExecutionBackend",
    max_concurrency: "int",
    drain_timeout: "float",
    *,
    app: "Litestar | None" = None,
) -> "int":
    from litestar_queues.worker.runtime import ShutdownWatchdog

    service = _open_service(plugin)
    await service.open()
    watchdog = ShutdownWatchdog(plugin.config.worker.hard_exit_timeout)
    stop_count = 0
    try:
        async with _enter_standalone_lifespan(plugin, app, service):
            task = asyncio.create_task(
                backend.run_consumer(service, max_concurrency=max_concurrency, drain_timeout=drain_timeout)
            )
            loop = asyncio.get_running_loop()

            def stop(signum: "int" = int(signal.SIGTERM)) -> "None":
                nonlocal stop_count
                stop_count += 1
                if stop_count > FORCE_STOP_SIGNAL_COUNT:
                    watchdog.exit_now(signum)
                    return
                if stop_count >= FORCE_STOP_SIGNAL_COUNT:
                    watchdog.arm(signum)
                task.cancel()

            try:
                for sig in (signal.SIGTERM, signal.SIGINT):
                    loop.add_signal_handler(sig, partial(stop, int(sig)))
            except NotImplementedError:
                for sig in (signal.SIGTERM, signal.SIGINT):
                    signal.signal(sig, lambda handled, _frame: stop(int(handled)))
            await task
    except asyncio.CancelledError:
        return 2 if stop_count > 1 else 0
    finally:
        watchdog.disarm()
        await service.close()
    return 0


@queues_group.command(name="status", help="Show queue status counts.")
@click.option("--queue", "queue_filter", default=None, help="Count only records in this queue.")
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable JSON.")
def status_command(ctx: "click.Context", queue_filter: "str | None", as_json: "bool") -> "None":
    env = _ensure_env(ctx)
    plugin = _resolve_plugin(env)
    _reject_ephemeral_storage(plugin, "status")
    exit_code = asyncio.run(_status_run(plugin, queue_filter, as_json))
    ctx.exit(exit_code)


@queues_group.command(name="list", help="List queued task records with filtering and pagination.")
@click.option(
    "--status",
    "statuses",
    multiple=True,
    type=click.Choice(_TASK_STATUS_CHOICES),
    help="Filter by task status. Repeatable.",
)
@click.option("--queue", "queue_filter", default=None, help="Filter by logical queue name.")
@click.option("--task-name", "task_name", default=None, help="Filter by registered task name.")
@click.option("--key", "dedup_key", default=None, help="Filter by deduplication key.")
@click.option("--limit", type=click.IntRange(min=1), default=50, help="Maximum records to return (default 50).")
@click.option("--offset", type=click.IntRange(min=0), default=0, help="Zero-based starting record offset (default 0).")
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable JSON.")
def list_command(
    ctx: "click.Context",
    *,
    statuses: "tuple[str, ...]",
    queue_filter: "str | None",
    task_name: "str | None",
    dedup_key: "str | None",
    limit: "int",
    offset: "int",
    as_json: "bool",
) -> "None":
    """List queued task records matching optional filters."""
    env = _ensure_env(ctx)
    plugin = _resolve_plugin(env)
    _reject_ephemeral_storage(plugin, "list")
    exit_code = asyncio.run(
        _list_run(
            plugin,
            statuses=statuses,
            queue_filter=queue_filter,
            task_name=task_name,
            key=dedup_key,
            limit=limit,
            offset=offset,
            as_json=as_json,
        )
    )
    ctx.exit(exit_code)


@queues_group.command(
    name="scheduler-health", help="Exit non-zero if the scheduler canary task has not completed within the window."
)
@click.option("--minutes", type=click.IntRange(min=1), default=5, help="Staleness threshold in minutes (default 5).")
def scheduler_health_command(ctx: "click.Context", minutes: "int") -> "None":
    env = _ensure_env(ctx)
    plugin = _resolve_plugin(env)
    _reject_ephemeral_storage(plugin, "scheduler-health")
    exit_code = asyncio.run(_scheduler_health_run(plugin, minutes))
    ctx.exit(exit_code)


@queues_group.command(
    name="run-task",
    help="Run one queued record by id (external-executor consumer). By default reads the task id "
    "QUEUES_TASK_ID and QUEUES_CONFIG_FACTORY from the environment; the options below "
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
    type=click.Choice(["expire", "external", "stale", "schedules", "terminal", "events"]),
    help="Maintenance phase to run. Repeatable; defaults to every configured phase. Only narrows configuration.",
)
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable JSON.")
def run_maintenance_command(ctx: "click.Context", phases: "tuple[str, ...]", as_json: "bool") -> "None":
    env = _ensure_env(ctx)
    plugin = _resolve_plugin(env)
    _reject_ephemeral_storage(plugin, "run-maintenance")
    exit_code = asyncio.run(_maintain_run(plugin, phases, as_json, app=env.app))
    ctx.exit(exit_code)


def register(cli: "click.Group") -> "None":
    """Attach the ``queues`` subcommand group to ``cli`` (idempotent)."""
    if queues_group.name not in cli.commands:
        cli.add_command(queues_group)


async def _maintain_run(
    plugin: "QueuePlugin", phases: "tuple[str, ...]", as_json: "bool", *, app: "Litestar | None" = None
) -> "int":
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
        async with _enter_standalone_lifespan(plugin, app, service):
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
        if phase.repair is not None:
            repair = phase.repair
            click.echo(
                f"  repair: examined={repair.examined} changed={repair.changed} "
                f"failed={repair.failed} unchanged={repair.unchanged} "
                f"limit_reached={str(repair.limit_reached).lower()}"
            )


def _maintenance_exit_code(summary: "QueueMaintenanceSummary") -> "int":
    if summary.outcome == "failed":
        return 1
    if summary.outcome == "partial":
        return 2
    return 0


def _failure_detail(exc: "BaseException") -> "str":
    """Describe a worker failure by stage and exception type only.

    Returns:
        ``"<stage> (<ExceptionType>)"`` for a staged startup failure, otherwise
        the exception type name.
    """
    stage = getattr(exc, "stage", None)
    exception_type = getattr(exc, "exception_type", None)
    if stage is not None and isinstance(exception_type, str):
        return f"{stage.value} ({exception_type})"
    return type(exc).__name__


async def _run_worker(
    plugin: "QueuePlugin",
    max_concurrency: "int",
    drain_timeout: "float",
    queues: "tuple[str, ...]" = (),
    *,
    app: "Litestar | None" = None,
) -> "int":
    """Run one standalone worker until it is signalled to stop.

    Returns:
        The process exit code: 0 clean, 1 crashed, 2 escalated.
    """
    from litestar_queues.worker.runtime import ShutdownWatchdog, WorkerRunResult, run_worker

    config = replace(
        plugin.config,
        worker=replace(
            plugin.config.worker,
            max_concurrency=max_concurrency,
            graceful_shutdown_timeout=drain_timeout,
            queues=queues,
        ),
    )
    watchdog = ShutdownWatchdog(config.worker.hard_exit_timeout)
    stop = _CLIStopCoordinator(watchdog=watchdog)
    loop = asyncio.get_running_loop()

    def _register_signal_handler(sig: "signal.Signals") -> "None":
        try:
            loop.add_signal_handler(sig, partial(stop.request_stop, int(sig)))
        except NotImplementedError:
            signal.signal(sig, lambda handled, _frame: stop.request_stop(int(handled)))

    for sig in (signal.SIGTERM, signal.SIGINT):
        _register_signal_handler(sig)

    def announce_ready() -> "None":
        click.echo("litestar queues worker started", err=True)

    try:
        async with plugin.open_worker_service(app) as service:
            result = await run_worker(
                service, config, graceful_stop=stop.graceful, force_stop=stop.force, ready=announce_ready
            )
    except Exception as exc:
        click.echo(f"error: queue worker failed during {_failure_detail(exc)}", err=True)
        logging.getLogger(config.names.logger("cli")).exception("Standalone queue worker failed")
        return int(WorkerRunResult.CRASHED)
    finally:
        watchdog.disarm()
    return int(result)


class _CLIStopCoordinator:
    """Translate repeated termination signals into the runner's two stop events.

    The second signal must reach the runner while the first drain is still in
    flight, so the force request is never queued behind the graceful one. It
    also arms the hard-exit watchdog, and a third signal exits at once: by then
    the operator has asked three times and the loop is demonstrably stuck.
    """

    __slots__ = ("force", "graceful", "stop_count", "watchdog")

    def __init__(self, watchdog: "ShutdownWatchdog | None" = None) -> "None":
        self.graceful = asyncio.Event()
        self.force = asyncio.Event()
        self.stop_count = 0
        self.watchdog = watchdog

    def request_stop(self, signum: "int" = signal.SIGTERM) -> "None":
        self.stop_count += 1
        if self.stop_count > FORCE_STOP_SIGNAL_COUNT and self.watchdog is not None:
            self.watchdog.exit_now(signum)
            return
        if self.stop_count >= FORCE_STOP_SIGNAL_COUNT:
            self.force.set()
            if self.watchdog is not None:
                self.watchdog.arm(signum)
        self.graceful.set()


async def _status_run(plugin: "QueuePlugin", queue_filter: "str | None", as_json: "bool") -> "int":
    service = _open_service(plugin)
    await service.open()
    try:
        stats = await service.get_statistics(queue=queue_filter)
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
        "expired": stats.expired,
        "total": stats.total,
    }

    if as_json:
        click.echo(json.dumps(payload, separators=(",", ":")))
    else:
        click.echo(f"{'Status':<12}{'Count':>8}")
        click.echo(f"{'-' * 12}{'-' * 8:>8}")
        for key in ("pending", "scheduled", "running", "completed", "failed", "cancelled", "expired"):
            click.echo(f"{key:<12}{payload[key]:>8}")
        click.echo(f"{'total':<12}{payload['total']:>8}")
    return 0


async def _list_run(
    plugin: "QueuePlugin",
    *,
    statuses: "tuple[str, ...]",
    queue_filter: "str | None",
    task_name: "str | None",
    key: "str | None",
    limit: "int",
    offset: "int",
    as_json: "bool",
) -> "int":
    """Query ``QueueService.list_tasks`` and emit either JSON or a summary table.

    Forwards ``statuses or None`` so an omitted ``--status`` flag queries every
    status instead of hitting the empty-sequence fast path.
    """
    service = _open_service(plugin)
    try:
        await service.open()
        page = await service.list_tasks(
            status=cast("tuple[TaskStatus, ...] | None", statuses or None),
            queue=queue_filter,
            task_name=task_name,
            key=key,
            limit=limit,
            offset=offset,
        )
    except Exception as exc:
        click.echo(f"error: {exc}", err=True)
        return 1
    finally:
        await service.close()

    if as_json:
        payload: "dict[str, Any]" = {
            "items": [_serialize_task_summary(record) for record in page.items],
            "limit": page.limit,
            "offset": page.offset,
            "total": page.total,
            "has_more": page.has_more,
        }
        click.echo(json.dumps(payload, separators=(",", ":")))
    else:
        _emit_task_list_table(
            page.items, total=page.total, limit=page.limit, offset=page.offset, has_more=page.has_more
        )
    return 0


def _iso_or_none(value: "datetime | None") -> "str | None":
    return value.isoformat() if value is not None else None


def _serialize_task_summary(record: "QueuedTaskRecord") -> "dict[str, Any]":
    """Serialize scalar operational metadata only, omitting raw task payloads."""
    return {
        "id": str(record.id),
        "task_name": record.task_name,
        "queue": record.queue,
        "status": record.status,
        "key": record.key,
        "priority": record.priority,
        "retry_count": record.retry_count,
        "max_retries": record.max_retries,
        "execution_backend": record.execution_backend,
        "execution_profile": record.execution_profile,
        "execution_ref": record.execution_ref,
        "worker_id": record.worker_id,
        "created_at": _iso_or_none(record.created_at),
        "queued_at": _iso_or_none(record.queued_at),
        "scheduled_at": _iso_or_none(record.scheduled_at),
        "started_at": _iso_or_none(record.started_at),
        "completed_at": _iso_or_none(record.completed_at),
        "heartbeat_at": _iso_or_none(record.heartbeat_at),
        "expires_at": _iso_or_none(record.expires_at),
        "error": record.error,
    }


def _emit_task_list_table(
    items: "Sequence[QueuedTaskRecord]", *, total: "int", limit: "int", offset: "int", has_more: "bool"
) -> "None":
    if not items:
        click.echo(f"no tasks found (total={total}, limit={limit}, offset={offset}, has_more={str(has_more).lower()})")
        return
    click.echo(f"{'ID':<36}  {'Status':<10}  {'Queue':<14}  {'Task':<26}  {'Created':<20}  Key")
    click.echo(f"{'-' * 36}  {'-' * 10}  {'-' * 14}  {'-' * 26}  {'-' * 20}  {'-' * 3}")
    for record in items:
        created = record.created_at.strftime("%Y-%m-%dT%H:%M:%SZ")
        click.echo(
            f"{record.id!s:<36}  {record.status:<10}  {record.queue:<14}  "
            f"{record.task_name:<26}  {created:<20}  {record.key or '-'}"
        )
    click.echo(f"total={total} limit={limit} offset={offset} has_more={str(has_more).lower()}")


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

    since = datetime.now(UTC) - timedelta(minutes=minutes)
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

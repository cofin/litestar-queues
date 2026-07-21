"""Framework-agnostic consumer core for external execution backends.

Click-free on purpose: broker consumers and the ``litestar queues`` subcommands
import ``consume_one`` / ``run_dispatched_task`` without pulling
``click`` into the module graph (see test_plugin_lifecycle import boundary).
"""

import asyncio
import contextlib
import logging
import os
from enum import IntEnum
from importlib import import_module
from typing import TYPE_CHECKING, cast
from uuid import UUID

from litestar_queues.config import QueueConfig
from litestar_queues.exceptions import QueueConfigurationError
from litestar_queues.execution.dispatch import TaskDispatch
from litestar_queues.models import HeartbeatTouch
from litestar_queues.service import QueueService
from litestar_queues.task import load_task_modules

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Mapping
    from contextlib import AbstractAsyncContextManager

    from litestar_queues.models import QueuedTaskRecord

    ServiceFactory = Callable[[], QueueConfig | QueueService | AbstractAsyncContextManager[QueueService]]

__all__ = ("DispatchExitCode", "consume_one", "run_dispatched_task")
logger = logging.getLogger(__name__)

_TASK_DISPATCH_ENV_SUFFIX = "TASK_DISPATCH"


class DispatchExitCode(IntEnum):
    """Deterministic external-consumer process exit codes."""

    SUCCESS = 0
    FAILURE = 1
    MISSING_DISPATCH = 2
    INVALID_DISPATCH = 3
    MISSING_RECORD = 4
    UNKNOWN_TASK = 5
    CLAIM_LOST = 6
    CANCELLED = 7
    MISSING_CONFIG_FACTORY = 8


async def consume_one(queue: "QueueService", dispatch: "TaskDispatch") -> "DispatchExitCode":
    """Claim, execute, and report one dispatched record identified by a task dispatch.

    The live record in the queue backend is authoritative; the dispatch only
    locates it. Redelivery is fenced by the live ``expected_retry_count`` at
    claim time.

    Returns:
        A deterministic dispatch exit code.
    """
    try:
        task_id = UUID(dispatch.task_id)
    except ValueError:
        return DispatchExitCode.INVALID_DISPATCH

    record = await queue.get_task(task_id)
    if record is None:
        return DispatchExitCode.MISSING_RECORD

    try:
        queue.resolve_task(record.task_name)
    except KeyError:
        await queue.get_queue_backend().fail_task(record.id, f"Unknown queue task: {record.task_name!r}", retry=False)
        return DispatchExitCode.UNKNOWN_TASK

    claimed = await queue.get_queue_backend().claim_task(record.id)
    if claimed is None:
        await queue.publish_claim_lost(record, phase="claim")
        return DispatchExitCode.CLAIM_LOST

    return await _execute_claimed_record(queue, claimed)


async def run_dispatched_task(
    *,
    config: "QueueConfig | None" = None,
    service: "QueueService | None" = None,
    service_factory: "ServiceFactory | None" = None,
    task_id: "str | None" = None,
    dispatch: "str | None" = None,
    config_factory: "str | None" = None,
    task_modules: "str | None" = None,
    env: "Mapping[str, str] | None" = None,
) -> "DispatchExitCode":
    """Resolve a service and run one dispatched task.

    The prefix-aware environment is the default source for every input; the
    override arguments take precedence over it. ``config_factory`` replaces the
    ``CONFIG_FACTORY`` env var, ``dispatch`` / ``task_id`` replace the
    ``TASK_DISPATCH`` payload, and ``task_modules`` replaces ``TASK_MODULES``.

    Returns:
        A deterministic dispatch exit code.
    """
    environ = env or os.environ
    if config_factory is not None:
        service_factory = _import_factory(config_factory)
    has_dispatch_override = dispatch is not None or task_id is not None

    if _requires_config_factory(
        config=config, service=service, service_factory=service_factory
    ) and not _has_config_factory(config, environ):
        if not has_dispatch_override and not environ.get(_env_name(config, _TASK_DISPATCH_ENV_SUFFIX)):
            return DispatchExitCode.MISSING_DISPATCH
        logger.error("External consumer process missing CONFIG_FACTORY")
        return DispatchExitCode.MISSING_CONFIG_FACTORY

    async with contextlib.AsyncExitStack() as stack:
        try:
            queue = await stack.enter_async_context(
                _provide_service(config=config, service=service, service_factory=service_factory, env=environ)
            )
        except Exception:
            if _requires_config_factory(config=config, service=service, service_factory=service_factory):
                logger.exception("External consumer process could not load CONFIG_FACTORY")
                return DispatchExitCode.MISSING_CONFIG_FACTORY
            raise

        _load_configured_task_modules(queue.config, environ, override=task_modules)
        return await _resolve_and_consume(queue, environ, task_id=task_id, dispatch=dispatch)


async def _resolve_and_consume(
    queue: "QueueService", env: "Mapping[str, str]", *, task_id: "str | None", dispatch: "str | None"
) -> "DispatchExitCode":
    if dispatch is not None:
        try:
            resolved = TaskDispatch.from_json(dispatch)
        except (ValueError, TypeError):
            return DispatchExitCode.INVALID_DISPATCH
        return await consume_one(queue, resolved)

    if task_id is not None:
        try:
            record_id = UUID(task_id)
        except ValueError:
            return DispatchExitCode.INVALID_DISPATCH
        record = await queue.get_task(record_id)
        if record is None:
            return DispatchExitCode.MISSING_RECORD
        return await consume_one(queue, TaskDispatch.from_record(record))

    raw = env.get(_env_name(queue.config, _TASK_DISPATCH_ENV_SUFFIX))
    if not raw:
        return DispatchExitCode.MISSING_DISPATCH
    try:
        resolved = TaskDispatch.from_json(raw)
    except (ValueError, TypeError):
        return DispatchExitCode.INVALID_DISPATCH
    return await consume_one(queue, resolved)


async def _execute_claimed_record(queue: "QueueService", claimed: "QueuedTaskRecord") -> "DispatchExitCode":
    expected_retry_count = claimed.retry_count
    heartbeat_task = asyncio.create_task(_heartbeat_loop(queue, claimed.id, expected_retry_count=expected_retry_count))
    execution_task = asyncio.create_task(queue.execute_record(claimed))
    try:
        done, _pending = await asyncio.wait({heartbeat_task, execution_task}, return_when=asyncio.FIRST_COMPLETED)
        if heartbeat_task in done and not heartbeat_task.result():
            execution_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await execution_task
            await queue.publish_claim_lost(claimed, phase="heartbeat", expected_retry_count=expected_retry_count)
            return DispatchExitCode.CLAIM_LOST
        updated = await execution_task
    except asyncio.CancelledError:
        return DispatchExitCode.CANCELLED
    finally:
        heartbeat_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await heartbeat_task
        await queue.get_queue_backend().null_heartbeats([claimed.id], expected_retry_count=expected_retry_count)

    if updated.status == "completed":
        return DispatchExitCode.SUCCESS
    if updated.status == "cancelled":
        return DispatchExitCode.CANCELLED
    return DispatchExitCode.FAILURE


async def _heartbeat_loop(queue: "QueueService", task_id: "UUID", *, expected_retry_count: "int") -> "bool":
    interval = queue.config.worker_heartbeat_interval
    while True:
        await asyncio.sleep(interval)
        result = await queue.get_queue_backend().touch_heartbeats([
            HeartbeatTouch(task_id=task_id, expected_retry_count=expected_retry_count)
        ])
        if task_id not in result.touched_task_ids:
            return False


@contextlib.asynccontextmanager
async def _provide_service(
    *,
    config: "QueueConfig | None",
    service: "QueueService | None",
    service_factory: "ServiceFactory | None",
    env: "Mapping[str, str]",
) -> "AsyncIterator[QueueService]":
    if service is not None:
        yield service
        return

    factory = service_factory or _load_config_factory(config, env)
    if factory is not None:
        provided = factory()
        if isinstance(provided, QueueConfig):
            async with QueueService(provided) as queue:
                yield queue
            return
        if isinstance(provided, QueueService):
            async with provided as queue:
                yield queue
            return
        async with provided as queue:
            yield queue
        return

    if config is None:
        msg = "External consumer process requires CONFIG_FACTORY when no QueueConfig or QueueService is provided."
        raise QueueConfigurationError(msg)

    async with QueueService(config) as queue:
        yield queue


def _requires_config_factory(
    *, config: "QueueConfig | None", service: "QueueService | None", service_factory: "ServiceFactory | None"
) -> "bool":
    return config is None and service is None and service_factory is None


def _has_config_factory(config: "QueueConfig | None", env: "Mapping[str, str]") -> "bool":
    return bool(env.get(_env_name(config, "CONFIG_FACTORY")))


def _load_config_factory(config: "QueueConfig | None", env: "Mapping[str, str]") -> "ServiceFactory | None":
    import_path = env.get(_env_name(config, "CONFIG_FACTORY"))
    if not import_path:
        return None
    return _import_factory(import_path)


def _import_factory(import_path: "str") -> "ServiceFactory":
    module_path, separator, attribute = import_path.partition(":")
    if not separator:
        module_path, attribute = import_path.rsplit(".", 1)
    module = import_module(module_path)
    factory = getattr(module, attribute)
    if not callable(factory):
        msg = f"Consumer config factory {import_path!r} is not callable."
        raise TypeError(msg)
    return cast("ServiceFactory", factory)


def _load_configured_task_modules(
    config: "QueueConfig", env: "Mapping[str, str]", *, override: "str | None" = None
) -> "None":
    modules = list(config.task_modules)
    extra = override if override is not None else env.get(_env_name(config, "TASK_MODULES"))
    if extra:
        modules.extend(module.strip() for module in extra.split(",") if module.strip())
    if modules:
        load_task_modules(tuple(modules), force_reload=True)


def _env_name(config: "QueueConfig | None", suffix: "str") -> "str":
    raw_config = config.execution_backend if config is not None else None
    env_name = getattr(raw_config, "env_name", None)
    if callable(env_name):
        return str(env_name(suffix))
    return f"LITESTAR_QUEUES_{suffix}"

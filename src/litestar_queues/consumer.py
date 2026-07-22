"""Public, framework-agnostic consumer API for external execution backends.

``run_task`` / ``consume_one`` / ``TaskExitCode`` are the programmatic twin of
``litestar queues run-task``: run one queued record by id on any external
executor (a Cloud Run Job, a serverless handler, a custom runner) and exit with
a deterministic code. Click-free on purpose so broker consumers and in-process
handlers can import them without pulling ``click`` into the module graph (see
test_plugin_lifecycle import boundary).
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
from litestar_queues.models import HeartbeatTouch
from litestar_queues.service import QueueService
from litestar_queues.task import load_task_modules

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Mapping
    from contextlib import AbstractAsyncContextManager

    from litestar_queues.models import QueuedTaskRecord

    ServiceFactory = Callable[[], QueueConfig | QueueService | AbstractAsyncContextManager[QueueService]]

__all__ = ("TaskExitCode", "consume_one", "run_task")
logger = logging.getLogger(__name__)

_TASK_ID_ENV_SUFFIX = "TASK_ID"


class TaskExitCode(IntEnum):
    """Deterministic external-consumer process exit codes."""

    SUCCESS = 0
    FAILURE = 1
    MISSING_TASK_ID = 2
    INVALID_TASK_ID = 3
    MISSING_RECORD = 4
    UNKNOWN_TASK = 5
    CLAIM_LOST = 6
    CANCELLED = 7
    MISSING_CONFIG_FACTORY = 8


async def consume_one(queue: "QueueService", task_id: "UUID") -> "TaskExitCode":
    """Claim, execute, and report one queued record identified by its id.

    The live record in the queue backend is authoritative; the id only locates
    it. Redelivery is fenced by the live ``expected_retry_count`` at claim time.

    Returns:
        A deterministic task exit code.
    """
    record = await queue.get_task(task_id)
    if record is None:
        return TaskExitCode.MISSING_RECORD

    try:
        queue.resolve_task(record.task_name)
    except KeyError:
        await queue.get_queue_backend().fail_task(record.id, f"Unknown queue task: {record.task_name!r}", retry=False)
        return TaskExitCode.UNKNOWN_TASK

    claimed = await queue.get_queue_backend().claim_task(record.id)
    if claimed is None:
        await queue.publish_claim_lost(record, phase="claim")
        return TaskExitCode.CLAIM_LOST

    return await _execute_claimed_record(queue, claimed)


async def run_task(
    *,
    config: "QueueConfig | None" = None,
    service: "QueueService | None" = None,
    service_factory: "ServiceFactory | None" = None,
    task_id: "str | None" = None,
    config_factory: "str | None" = None,
    task_modules: "str | None" = None,
    env: "Mapping[str, str] | None" = None,
) -> "TaskExitCode":
    """Resolve a service and run one queued task by id.

    The prefix-aware environment is the default source for every input; the
    override arguments take precedence over it. ``config_factory`` replaces the
    ``CONFIG_FACTORY`` env var, ``task_id`` replaces the ``TASK_ID`` value, and
    ``task_modules`` replaces ``TASK_MODULES``.

    Returns:
        A deterministic task exit code.
    """
    environ = env or os.environ
    if config_factory is not None:
        service_factory = _import_factory(config_factory)
    has_task_id_override = task_id is not None

    if _requires_config_factory(
        config=config, service=service, service_factory=service_factory
    ) and not _has_config_factory(config, environ):
        if not has_task_id_override and not environ.get(_env_name(config, _TASK_ID_ENV_SUFFIX)):
            return TaskExitCode.MISSING_TASK_ID
        logger.error("External consumer process missing CONFIG_FACTORY")
        return TaskExitCode.MISSING_CONFIG_FACTORY

    async with contextlib.AsyncExitStack() as stack:
        try:
            queue = await stack.enter_async_context(
                _provide_service(config=config, service=service, service_factory=service_factory, env=environ)
            )
        except Exception:
            if _requires_config_factory(config=config, service=service, service_factory=service_factory):
                logger.exception("External consumer process could not load CONFIG_FACTORY")
                return TaskExitCode.MISSING_CONFIG_FACTORY
            raise

        _load_configured_task_modules(queue.config, environ, override=task_modules)
        return await _resolve_and_consume(queue, environ, task_id=task_id)


async def _resolve_and_consume(
    queue: "QueueService", env: "Mapping[str, str]", *, task_id: "str | None"
) -> "TaskExitCode":
    raw = task_id if task_id is not None else env.get(_env_name(queue.config, _TASK_ID_ENV_SUFFIX))
    if not raw:
        return TaskExitCode.MISSING_TASK_ID
    try:
        record_id = UUID(raw)
    except ValueError:
        return TaskExitCode.INVALID_TASK_ID
    return await consume_one(queue, record_id)


async def _execute_claimed_record(queue: "QueueService", claimed: "QueuedTaskRecord") -> "TaskExitCode":
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
            return TaskExitCode.CLAIM_LOST
        updated = await execution_task
    except asyncio.CancelledError:
        return TaskExitCode.CANCELLED
    finally:
        heartbeat_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await heartbeat_task
        await queue.get_queue_backend().null_heartbeats([claimed.id], expected_retry_count=expected_retry_count)

    if updated.status == "completed":
        return TaskExitCode.SUCCESS
    if updated.status == "cancelled":
        return TaskExitCode.CANCELLED
    return TaskExitCode.FAILURE


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

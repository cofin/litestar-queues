"""Framework-agnostic consumer core for external execution backends.

Click-free on purpose: broker consumers and the ``litestar queues`` subcommands
import ``consume_one`` / ``run_config_factory_consumer`` without pulling
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
from litestar_queues.execution.envelope import DispatchEnvelope
from litestar_queues.models import HeartbeatTouch
from litestar_queues.service import QueueService
from litestar_queues.task import load_task_modules

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Mapping
    from contextlib import AbstractAsyncContextManager

    from litestar_queues.models import QueuedTaskRecord

    ServiceFactory = Callable[[], QueueConfig | QueueService | AbstractAsyncContextManager[QueueService]]

__all__ = ("ConsumerExitCode", "consume_one", "run_config_factory_consumer")
logger = logging.getLogger(__name__)

_DISPATCH_ENVELOPE_ENV_SUFFIX = "DISPATCH_ENVELOPE"


class ConsumerExitCode(IntEnum):
    """Deterministic external-consumer process exit codes."""

    SUCCESS = 0
    FAILURE = 1
    MISSING_ENVELOPE = 2
    INVALID_ENVELOPE = 3
    MISSING_RECORD = 4
    UNKNOWN_TASK = 5
    CLAIM_LOST = 6
    CANCELLED = 7
    MISSING_CONFIG_FACTORY = 8


async def consume_one(queue: "QueueService", envelope: "DispatchEnvelope") -> "ConsumerExitCode":
    """Claim, execute, and report one dispatched record identified by an envelope.

    The live record in the queue backend is authoritative; the envelope only
    locates it. Redelivery is fenced by the live ``expected_retry_count`` at
    claim time.

    Returns:
        A deterministic consumer exit code.
    """
    try:
        task_id = UUID(envelope.task_id)
    except ValueError:
        return ConsumerExitCode.INVALID_ENVELOPE

    record = await queue.get_task(task_id)
    if record is None:
        return ConsumerExitCode.MISSING_RECORD

    try:
        queue.resolve_task(record.task_name)
    except KeyError:
        await queue.get_queue_backend().fail_task(record.id, f"Unknown queue task: {record.task_name!r}", retry=False)
        return ConsumerExitCode.UNKNOWN_TASK

    claimed = await queue.get_queue_backend().claim_task(record.id)
    if claimed is None:
        await queue.publish_claim_lost(record, phase="claim")
        return ConsumerExitCode.CLAIM_LOST

    return await _execute_claimed_record(queue, claimed)


async def run_config_factory_consumer(
    *,
    config: "QueueConfig | None" = None,
    service: "QueueService | None" = None,
    service_factory: "ServiceFactory | None" = None,
    env: "Mapping[str, str] | None" = None,
) -> "ConsumerExitCode":
    """Resolve a service (via CONFIG_FACTORY) and consume one envelope from the environment.

    Returns:
        A deterministic consumer exit code.
    """
    environ = env or os.environ
    if _requires_config_factory(
        config=config, service=service, service_factory=service_factory
    ) and not _has_config_factory(config, environ):
        if not environ.get(_env_name(config, _DISPATCH_ENVELOPE_ENV_SUFFIX)):
            return ConsumerExitCode.MISSING_ENVELOPE
        logger.error("External consumer process missing CONFIG_FACTORY")
        return ConsumerExitCode.MISSING_CONFIG_FACTORY

    async with contextlib.AsyncExitStack() as stack:
        try:
            queue = await stack.enter_async_context(
                _provide_service(config=config, service=service, service_factory=service_factory, env=environ)
            )
        except Exception:
            if _requires_config_factory(config=config, service=service, service_factory=service_factory):
                logger.exception("External consumer process could not load CONFIG_FACTORY")
                return ConsumerExitCode.MISSING_CONFIG_FACTORY
            raise

        raw = environ.get(_env_name(queue.config, _DISPATCH_ENVELOPE_ENV_SUFFIX))
        if not raw:
            return ConsumerExitCode.MISSING_ENVELOPE
        try:
            envelope = DispatchEnvelope.from_json(raw)
        except (ValueError, TypeError):
            return ConsumerExitCode.INVALID_ENVELOPE

        _load_configured_task_modules(queue.config, environ)
        return await consume_one(queue, envelope)


async def _execute_claimed_record(queue: "QueueService", claimed: "QueuedTaskRecord") -> "ConsumerExitCode":
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
            return ConsumerExitCode.CLAIM_LOST
        updated = await execution_task
    except asyncio.CancelledError:
        return ConsumerExitCode.CANCELLED
    finally:
        heartbeat_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await heartbeat_task
        await queue.get_queue_backend().null_heartbeats([claimed.id], expected_retry_count=expected_retry_count)

    if updated.status == "completed":
        return ConsumerExitCode.SUCCESS
    if updated.status == "cancelled":
        return ConsumerExitCode.CANCELLED
    return ConsumerExitCode.FAILURE


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
    env_var = _env_name(config, "CONFIG_FACTORY")
    import_path = env.get(env_var)
    if not import_path:
        return None
    module_path, separator, attribute = import_path.partition(":")
    if not separator:
        module_path, attribute = import_path.rsplit(".", 1)
    module = import_module(module_path)
    factory = getattr(module, attribute)
    if not callable(factory):
        msg = f"Consumer config factory {import_path!r} is not callable."
        raise TypeError(msg)
    return cast("ServiceFactory", factory)


def _load_configured_task_modules(config: "QueueConfig", env: "Mapping[str, str]") -> "None":
    modules = list(config.task_modules)
    env_modules = env.get(_env_name(config, "TASK_MODULES"))
    if env_modules:
        modules.extend(module.strip() for module in env_modules.split(",") if module.strip())
    if modules:
        load_task_modules(tuple(modules), force_reload=True)


def _env_name(config: "QueueConfig | None", suffix: "str") -> "str":
    raw_config = config.execution_backend if config is not None else None
    env_name = getattr(raw_config, "env_name", None)
    if callable(env_name):
        return str(env_name(suffix))
    return f"LITESTAR_QUEUES_{suffix}"

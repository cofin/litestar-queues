import asyncio
import contextlib
import logging
import os
from enum import IntEnum
from importlib import import_module
from typing import TYPE_CHECKING, Any, cast
from uuid import UUID

from litestar_queues.config import QueueConfig
from litestar_queues.exceptions import QueueConfigurationError
from litestar_queues.service import QueueService
from litestar_queues.task import load_task_modules

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Mapping

    from litestar_queues.models import QueuedTaskRecord

__all__ = ("CloudRunExitCode", "execute_cloudrun_task", "main")
logger = logging.getLogger(__name__)


class CloudRunExitCode(IntEnum):
    """Deterministic Cloud Run task process exit codes."""

    SUCCESS = 0
    FAILURE = 1
    MISSING_TASK_ID = 2
    INVALID_TASK_ID = 3
    MISSING_RECORD = 4
    UNKNOWN_TASK = 5
    CLAIM_LOST = 6
    CANCELLED = 7
    MISSING_CONFIG_FACTORY = 8


async def execute_cloudrun_task(
    *,
    config: "QueueConfig | None" = None,
    service: "QueueService | None" = None,
    service_factory: "Callable[[], Any] | None" = None,
    env: "Mapping[str, str] | None" = None,
) -> "CloudRunExitCode":
    """Execute one persisted queue record in a Cloud Run task process.

    Returns:
        A deterministic process exit code.
    """
    environ = env or os.environ
    if _requires_config_factory(
        config=config, service=service, service_factory=service_factory
    ) and not _has_config_factory(config, environ):
        task_id_raw = environ.get(_env_name(config, "TASK_ID"))
        if not task_id_raw:
            return CloudRunExitCode.MISSING_TASK_ID
        try:
            UUID(task_id_raw)
        except ValueError:
            return CloudRunExitCode.INVALID_TASK_ID
        logger.error("Cloud Run task process missing CONFIG_FACTORY", extra={"cloudrun_task_id": task_id_raw})
        return CloudRunExitCode.MISSING_CONFIG_FACTORY

    async with contextlib.AsyncExitStack() as stack:
        try:
            queue = await stack.enter_async_context(
                _provide_service(config=config, service=service, service_factory=service_factory, env=environ)
            )
        except Exception:
            if _requires_config_factory(config=config, service=service, service_factory=service_factory):
                logger.exception("Cloud Run task process could not load CONFIG_FACTORY")
                return CloudRunExitCode.MISSING_CONFIG_FACTORY
            raise

        task_id_raw = environ.get(_env_name(queue.config, "TASK_ID"))
        if not task_id_raw:
            return CloudRunExitCode.MISSING_TASK_ID
        try:
            task_id = UUID(task_id_raw)
        except ValueError:
            return CloudRunExitCode.INVALID_TASK_ID

        _load_configured_task_modules(queue.config, environ)
        record = await queue.get_task(task_id)
        if record is None:
            return CloudRunExitCode.MISSING_RECORD

        try:
            queue.resolve_task(record.task_name)
        except KeyError:
            await queue.get_queue_backend().fail_task(
                record.id, f"Unknown queue task: {record.task_name!r}", retry=False
            )
            return CloudRunExitCode.UNKNOWN_TASK

        claimed = await queue.get_queue_backend().claim_task(record.id)
        if claimed is None:
            await queue.publish_claim_lost(record, phase="claim")
            return CloudRunExitCode.CLAIM_LOST

        return await _execute_claimed_record(queue, claimed)


def main() -> "None":
    """Console entry point for Cloud Run task execution.

    Raises:
        SystemExit: Always raised with the execution exit code.
    """
    raise SystemExit(int(asyncio.run(execute_cloudrun_task())))


async def _execute_claimed_record(queue: "QueueService", claimed: "QueuedTaskRecord") -> "CloudRunExitCode":
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
            return CloudRunExitCode.CLAIM_LOST
        updated = await execution_task
    except asyncio.CancelledError:
        return CloudRunExitCode.CANCELLED
    finally:
        heartbeat_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await heartbeat_task
        await queue.get_queue_backend().null_heartbeats([claimed.id], expected_retry_count=expected_retry_count)

    if updated.status == "completed":
        return CloudRunExitCode.SUCCESS
    if updated.status == "cancelled":
        return CloudRunExitCode.CANCELLED
    return CloudRunExitCode.FAILURE


async def _heartbeat_loop(queue: "QueueService", task_id: "UUID", *, expected_retry_count: "int") -> "bool":
    interval = queue.config.worker_heartbeat_interval
    while True:
        await asyncio.sleep(interval)
        if not await queue.get_queue_backend().touch_heartbeat(task_id, expected_retry_count=expected_retry_count):
            return False


@contextlib.asynccontextmanager
async def _provide_service(
    *,
    config: "QueueConfig | None",
    service: "QueueService | None",
    service_factory: "Callable[[], Any] | None",
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
        msg = "Cloud Run task process requires CONFIG_FACTORY when no QueueConfig or QueueService is provided."
        raise QueueConfigurationError(msg)

    async with QueueService(config) as queue:
        yield queue


def _requires_config_factory(
    *, config: "QueueConfig | None", service: "QueueService | None", service_factory: "Callable[[], Any] | None"
) -> "bool":
    return config is None and service is None and service_factory is None


def _has_config_factory(config: "QueueConfig | None", env: "Mapping[str, str]") -> "bool":
    return bool(env.get(_env_name(config, "CONFIG_FACTORY")))


def _load_config_factory(config: "QueueConfig | None", env: "Mapping[str, str]") -> "Callable[[], Any] | None":
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
        msg = f"Cloud Run config factory {import_path!r} is not callable."
        raise TypeError(msg)
    return cast("Callable[[], Any]", factory)


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


if __name__ == "__main__":
    main()

import asyncio
import contextlib
import os
import sys
from collections.abc import AsyncIterator, Callable, Mapping
from enum import IntEnum
from importlib import import_module
from typing import Any, cast
from uuid import UUID

from litestar_queues.config import QueueConfig
from litestar_queues.service import QueueService
from litestar_queues.task import load_task_modules

__all__ = ("CloudRunExitCode", "execute_cloudrun_task", "main")


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


async def execute_cloudrun_task(
    *,
    config: QueueConfig | None = None,
    service: QueueService | None = None,
    service_factory: Callable[[], Any] | None = None,
    env: Mapping[str, str] | None = None,
) -> CloudRunExitCode:
    """Execute one persisted queue record in a Cloud Run task process."""
    environ = env or os.environ
    task_id_raw = environ.get(_env_name(config, "TASK_ID"))
    if not task_id_raw:
        return CloudRunExitCode.MISSING_TASK_ID
    try:
        task_id = UUID(task_id_raw)
    except ValueError:
        return CloudRunExitCode.INVALID_TASK_ID

    async with _provide_service(config=config, service=service, service_factory=service_factory, env=environ) as queue:
        _load_configured_task_modules(queue.config, environ)
        record = await queue.get_task(task_id)
        if record is None:
            return CloudRunExitCode.MISSING_RECORD

        try:
            queue.resolve_task(record.task_name)
        except KeyError:
            await queue.get_queue_backend().fail_task(record.id, f"Unknown queue task: {record.task_name!r}", retry=False)
            return CloudRunExitCode.UNKNOWN_TASK

        claimed = await queue.get_queue_backend().claim_task(record.id)
        if claimed is None:
            return CloudRunExitCode.CLAIM_LOST

        heartbeat_task = asyncio.create_task(_heartbeat_loop(queue, claimed.id))
        try:
            updated = await queue.execute_record(claimed)
        except asyncio.CancelledError:
            return CloudRunExitCode.CANCELLED
        finally:
            heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat_task
            await queue.get_queue_backend().null_heartbeats([claimed.id])

    if updated.status == "completed":
        return CloudRunExitCode.SUCCESS
    if updated.status == "cancelled":
        return CloudRunExitCode.CANCELLED
    return CloudRunExitCode.FAILURE


def main() -> None:
    """Console entry point for Cloud Run task execution."""
    raise SystemExit(int(asyncio.run(execute_cloudrun_task())))


async def _heartbeat_loop(queue: QueueService, task_id: UUID) -> None:
    interval = queue.config.worker_heartbeat_interval
    while True:
        await asyncio.sleep(interval)
        await queue.get_queue_backend().touch_heartbeat(task_id)


@contextlib.asynccontextmanager
async def _provide_service(
    *,
    config: QueueConfig | None,
    service: QueueService | None,
    service_factory: Callable[[], Any] | None,
    env: Mapping[str, str],
) -> AsyncIterator[QueueService]:
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

    async with QueueService(config or QueueConfig()) as queue:
        yield queue


def _load_config_factory(config: QueueConfig | None, env: Mapping[str, str]) -> Callable[[], Any] | None:
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


def _load_configured_task_modules(config: QueueConfig, env: Mapping[str, str]) -> None:
    modules = list(config.task_modules)
    env_modules = env.get(_env_name(config, "TASK_MODULES"))
    if env_modules:
        modules.extend(module.strip() for module in env_modules.split(",") if module.strip())
    if modules:
        load_task_modules(tuple(modules), force_reload=True)


def _env_name(config: QueueConfig | None, suffix: str) -> str:
    raw_config = config.execution_backend_config if config is not None else {}
    if isinstance(raw_config, dict) and "cloudrun" in raw_config:
        raw_config = raw_config["cloudrun"]
    env_name = getattr(raw_config, "env_name", None)
    if callable(env_name):
        return str(env_name(suffix))
    return f"LITESTAR_QUEUES_{suffix}"


if __name__ == "__main__":
    main()

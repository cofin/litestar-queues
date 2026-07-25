"""Click-free orchestration for one queue worker."""

import asyncio
import contextlib
from enum import Enum, IntEnum
from typing import TYPE_CHECKING, Protocol

from litestar_queues.task import load_task_modules
from litestar_queues.worker import Worker

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import NoReturn

    from litestar_queues.config import QueueConfig, WorkerConfig
    from litestar_queues.service import QueueService

__all__ = ()


class WorkerRunResult(IntEnum):
    """Private process result used by worker entry points."""

    CLEAN = 0
    CRASHED = 1
    ESCALATED = 2


class _WorkerStage(str, Enum):
    """Startup stages safe to report across a process boundary."""

    LOAD_TASKS = "load_tasks"
    OPEN_SERVICE = "open_service"
    INITIALIZE_SCHEDULES = "initialize_schedules"
    START_WORKER = "start_worker"


class _WorkerStageError(Exception):
    """Sanitized startup failure retaining its original only as a cause."""

    def __init__(self, stage: "_WorkerStage", cause: "Exception") -> "None":
        self.stage = stage
        self.exception_type = type(cause).__name__
        super().__init__(stage.value, self.exception_type)
        self.__cause__ = cause


class _WorkerLike(Protocol):
    async def start(self) -> "None": ...

    async def wait_started(self) -> "None": ...

    async def stop(self, *, force: "bool" = False) -> "bool": ...


class _WorkerFactory(Protocol):
    # Positional-only: these are internal injection seams, always called
    # positionally, so a substitute need not match parameter names.
    def __call__(self, service: "QueueService", config: "WorkerConfig", /) -> "_WorkerLike": ...


class _TaskLoader(Protocol):
    def __call__(self, modules: "tuple[str, ...]", /) -> "int": ...


def _create_worker(service: "QueueService", config: "WorkerConfig") -> "_WorkerLike":
    return Worker(service, config)


def _load_tasks(modules: "tuple[str, ...]") -> "int":
    return load_task_modules(modules)


def _raise_stage_error(stage: "_WorkerStage", cause: "Exception") -> "NoReturn":
    raise _WorkerStageError(stage, cause) from cause


async def _cancel_tasks(*tasks: "asyncio.Task[object] | None") -> "None":
    active = [task for task in tasks if task is not None]
    for task in active:
        if not task.done():
            task.cancel()
    if active:
        await asyncio.gather(*active, return_exceptions=True)


def _task_exception(task: "asyncio.Task[None]") -> "Exception | None":
    exception = task.exception()
    if exception is None:
        return None
    if isinstance(exception, Exception):
        return exception
    raise exception


def _task_crashed(task: "asyncio.Task[None]") -> "bool":
    return _task_exception(task) is not None


async def _inspect_startup_tasks(readiness_task: "asyncio.Task[None]", worker_task: "asyncio.Task[None]") -> "bool":
    readiness_succeeded = False
    if readiness_task.done():
        try:
            await readiness_task
        except Exception as exc:  # noqa: BLE001 - startup failures are sanitized at this boundary.
            _raise_stage_error(_WorkerStage.START_WORKER, exc)
        readiness_succeeded = True
    if worker_task.done():
        worker_error = _task_exception(worker_task)
        if worker_error is not None:
            _raise_stage_error(_WorkerStage.START_WORKER, worker_error)
        if not readiness_succeeded:
            error = RuntimeError("Worker exited before startup readiness")
            _raise_stage_error(_WorkerStage.START_WORKER, error)
    return readiness_succeeded


def _publish_ready(ready: "Callable[[], None] | None") -> "None":
    try:
        if ready is not None:
            ready()
    except Exception as exc:  # noqa: BLE001 - ready publication is part of startup.
        _raise_stage_error(_WorkerStage.START_WORKER, exc)


async def _force_worker(worker: "_WorkerLike", worker_task: "asyncio.Task[None]") -> "WorkerRunResult":
    try:
        with contextlib.suppress(Exception):
            await worker.stop(force=True)
    finally:
        if not worker_task.done():
            worker_task.cancel()
        await asyncio.gather(worker_task, return_exceptions=True)
    return WorkerRunResult.ESCALATED


async def _wait_after_graceful_stop(
    worker: "_WorkerLike",
    worker_task: "asyncio.Task[None]",
    force_wait_task: "asyncio.Task[bool]",
    *,
    escalated: "bool",
) -> "WorkerRunResult":
    done, _ = await asyncio.wait((worker_task, force_wait_task), return_when=asyncio.FIRST_COMPLETED)
    if force_wait_task in done:
        return await _force_worker(worker, worker_task)
    if escalated:
        return WorkerRunResult.ESCALATED
    return WorkerRunResult.CRASHED if _task_crashed(worker_task) else WorkerRunResult.CLEAN


async def _gracefully_stop_worker(
    worker: "_WorkerLike",
    worker_task: "asyncio.Task[None]",
    force_wait_task: "asyncio.Task[bool]",
    *,
    startup_pending: "bool" = False,
) -> "WorkerRunResult":
    graceful_task = asyncio.create_task(worker.stop())
    try:
        done, _ = await asyncio.wait((graceful_task, worker_task, force_wait_task), return_when=asyncio.FIRST_COMPLETED)
        if force_wait_task in done:
            await _cancel_tasks(graceful_task)
            return await _force_worker(worker, worker_task)
        if graceful_task not in done:
            done, _ = await asyncio.wait((graceful_task, force_wait_task), return_when=asyncio.FIRST_COMPLETED)
            if force_wait_task in done:
                await _cancel_tasks(graceful_task)
                return await _force_worker(worker, worker_task)
        try:
            escalated = await graceful_task
        except Exception:  # noqa: BLE001 - a failed stop is a worker crash, not a startup-stage failure.
            return WorkerRunResult.CRASHED
        if startup_pending:
            if not worker_task.done():
                worker_task.cancel()
            await asyncio.gather(worker_task, return_exceptions=True)
            return WorkerRunResult.ESCALATED if escalated else WorkerRunResult.CLEAN
        if worker_task.done():
            if escalated:
                return WorkerRunResult.ESCALATED
            return WorkerRunResult.CRASHED if _task_crashed(worker_task) else WorkerRunResult.CLEAN
        return await _wait_after_graceful_stop(worker, worker_task, force_wait_task, escalated=escalated)
    finally:
        await _cancel_tasks(graceful_task)


async def _run_until_stopped(
    worker: "_WorkerLike",
    worker_task: "asyncio.Task[None]",
    graceful_wait_task: "asyncio.Task[bool]",
    force_wait_task: "asyncio.Task[bool]",
) -> "WorkerRunResult":
    done, _ = await asyncio.wait(
        (worker_task, graceful_wait_task, force_wait_task), return_when=asyncio.FIRST_COMPLETED
    )
    if force_wait_task in done:
        return await _force_worker(worker, worker_task)
    if graceful_wait_task in done:
        return await _gracefully_stop_worker(worker, worker_task, force_wait_task)
    _task_crashed(worker_task)
    return WorkerRunResult.CRASHED


async def _start_and_run_worker(
    worker: "_WorkerLike",
    *,
    graceful_stop: "asyncio.Event",
    force_stop: "asyncio.Event",
    ready: "Callable[[], None] | None",
) -> "WorkerRunResult":
    worker_task = asyncio.create_task(worker.start())
    readiness_task = asyncio.create_task(worker.wait_started())
    graceful_wait_task = asyncio.create_task(graceful_stop.wait())
    force_wait_task = asyncio.create_task(force_stop.wait())
    try:
        done, _ = await asyncio.wait(
            (readiness_task, worker_task, graceful_wait_task, force_wait_task), return_when=asyncio.FIRST_COMPLETED
        )
        if force_wait_task in done or graceful_wait_task in done:
            await asyncio.sleep(0)
        readiness_succeeded = await _inspect_startup_tasks(readiness_task, worker_task)
        if force_wait_task.done():
            return await _force_worker(worker, worker_task)
        if graceful_wait_task.done():
            return await _gracefully_stop_worker(
                worker, worker_task, force_wait_task, startup_pending=not readiness_succeeded
            )
        if readiness_succeeded:
            _publish_ready(ready)
            return await _run_until_stopped(worker, worker_task, graceful_wait_task, force_wait_task)
        error = RuntimeError("Worker startup wait completed without an outcome")
        _raise_stage_error(_WorkerStage.START_WORKER, error)
    finally:
        await _cancel_tasks(readiness_task, graceful_wait_task, force_wait_task)
        try:
            if not worker_task.done():
                with contextlib.suppress(Exception):
                    await worker.stop()
        finally:
            if not worker_task.done():
                worker_task.cancel()
            await asyncio.gather(worker_task, return_exceptions=True)


def _load_worker_tasks(config: "QueueConfig", task_loader: "_TaskLoader") -> "None":
    try:
        task_loader(config.task_modules)
    except Exception as exc:  # noqa: BLE001 - startup failures are sanitized at this boundary.
        _raise_stage_error(_WorkerStage.LOAD_TASKS, exc)


async def _open_worker_service(service: "QueueService") -> "None":
    try:
        await service.open()
    except Exception as exc:  # noqa: BLE001 - startup failures are sanitized at this boundary.
        _raise_stage_error(_WorkerStage.OPEN_SERVICE, exc)


async def _initialize_worker_schedules(service: "QueueService") -> "None":
    try:
        await service.initialize_schedules()
    except Exception as exc:  # noqa: BLE001 - startup failures are sanitized at this boundary.
        _raise_stage_error(_WorkerStage.INITIALIZE_SCHEDULES, exc)


def _build_worker(service: "QueueService", config: "QueueConfig", worker_factory: "_WorkerFactory") -> "_WorkerLike":
    try:
        return worker_factory(service, config.worker)
    except Exception as exc:  # noqa: BLE001 - worker construction is part of startup.
        _raise_stage_error(_WorkerStage.START_WORKER, exc)


async def run_worker(
    service: "QueueService",
    config: "QueueConfig",
    *,
    graceful_stop: "asyncio.Event",
    force_stop: "asyncio.Event",
    ready: "Callable[[], None] | None" = None,
    _worker_factory: "_WorkerFactory" = _create_worker,
    _task_loader: "_TaskLoader" = _load_tasks,
) -> "WorkerRunResult":
    """Run one worker and own its startup, stop, and service lifecycle."""
    service_opened = False
    result: "WorkerRunResult | None" = None
    primary_error: "BaseException | None" = None
    try:
        _load_worker_tasks(config, _task_loader)
        await _open_worker_service(service)
        service_opened = True
        if config.initialize_schedules:
            await _initialize_worker_schedules(service)
        worker = _build_worker(service, config, _worker_factory)
        result = await _start_and_run_worker(worker, graceful_stop=graceful_stop, force_stop=force_stop, ready=ready)
    except BaseException as exc:  # noqa: BLE001 - cleanup must run without masking this primary failure.
        primary_error = exc

    cleanup_error: "BaseException | None" = None
    if service_opened:
        try:
            await service.close()
        except BaseException as exc:  # noqa: BLE001 - cleanup precedence is resolved below.
            cleanup_error = exc

    if primary_error is not None:
        raise primary_error
    if result is None:
        msg = "Worker runner exited without a result"
        raise RuntimeError(msg)
    if cleanup_error is not None and (not isinstance(cleanup_error, Exception) or result is WorkerRunResult.CLEAN):
        raise cleanup_error
    return result

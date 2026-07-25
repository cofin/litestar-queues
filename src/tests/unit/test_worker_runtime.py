import asyncio
import subprocess
import sys
from typing import TYPE_CHECKING

import pytest

from litestar_queues import QueueConfig
from litestar_queues.config import WorkerConfig
from litestar_queues.worker.runtime import WorkerRunResult, _WorkerStage, _WorkerStageError, run_worker

if TYPE_CHECKING:
    from collections.abc import Callable

    from litestar_queues.service import QueueService

pytestmark = pytest.mark.anyio


class _FakeService:
    def __init__(
        self,
        order: "list[str]",
        *,
        open_error: "BaseException | None" = None,
        schedule_error: "BaseException | None" = None,
        close_error: "BaseException | None" = None,
    ) -> "None":
        self.order = order
        self.open_error = open_error
        self.schedule_error = schedule_error
        self.close_error = close_error
        self.schedule_calls = 0
        self.close_calls = 0

    async def open(self) -> "None":
        self.order.append("service.open")
        if self.open_error is not None:
            raise self.open_error

    async def initialize_schedules(self) -> "None":
        self.order.append("service.initialize_schedules")
        self.schedule_calls += 1
        if self.schedule_error is not None:
            raise self.schedule_error

    async def close(self) -> "None":
        self.order.append("service.close")
        self.close_calls += 1
        if self.close_error is not None:
            raise self.close_error


class _FakeWorker:
    def __init__(
        self,
        order: "list[str]",
        *,
        readiness_error: "BaseException | None" = None,
        start_error: "BaseException | None" = None,
        stop_result: "bool" = False,
        delay_readiness: "bool" = False,
        delay_graceful_stop: "bool" = False,
        delay_graceful_result: "bool" = False,
        crash_during_graceful_stop: "bool" = False,
        delay_force_stop: "bool" = False,
        force_stop_error: "BaseException | None" = None,
        graceful_stop_error: "BaseException | None" = None,
        post_readiness_error: "BaseException | None" = None,
    ) -> "None":
        self.order = order
        self.readiness_error = readiness_error
        self.start_error = start_error
        self.stop_result = stop_result
        self.crash_during_graceful_stop = crash_during_graceful_stop
        self.force_stop_error = force_stop_error
        self.graceful_stop_error = graceful_stop_error
        self.post_readiness_error = post_readiness_error
        self.allow_readiness = asyncio.Event()
        self.started = asyncio.Event()
        self.finish = asyncio.Event()
        self.crash = asyncio.Event()
        self.start_finished = asyncio.Event()
        self.stop_entered = asyncio.Event()
        self.wait_started_entered = asyncio.Event()
        self.allow_force_stop = asyncio.Event()
        self.allow_graceful_stop = asyncio.Event()
        self.allow_graceful_result = asyncio.Event()
        self.allow_post_readiness_error = asyncio.Event()
        self.graceful_stop_cancelled = False
        self.stop_calls: "list[bool]" = []
        if not delay_readiness:
            self.allow_readiness.set()
        if not delay_graceful_stop:
            self.allow_graceful_stop.set()
        if not delay_graceful_result:
            self.allow_graceful_result.set()
        if not delay_force_stop:
            self.allow_force_stop.set()

    async def start(self) -> "None":
        self.order.append("worker.start")
        try:
            if self.start_error is not None:
                raise self.start_error
            await self.allow_readiness.wait()
            self.started.set()
            if self.post_readiness_error is not None:
                await self.allow_post_readiness_error.wait()
                raise self.post_readiness_error
            finish_task = asyncio.create_task(self.finish.wait())
            crash_task = asyncio.create_task(self.crash.wait())
            try:
                done, _ = await asyncio.wait((finish_task, crash_task), return_when=asyncio.FIRST_COMPLETED)
                if crash_task in done:
                    msg = "worker crashed"
                    raise RuntimeError(msg)
            finally:
                for task in (finish_task, crash_task):
                    if not task.done():
                        task.cancel()
                await asyncio.gather(finish_task, crash_task, return_exceptions=True)
        finally:
            self.start_finished.set()

    async def wait_started(self) -> "None":
        self.order.append("worker.wait_started")
        self.wait_started_entered.set()
        await self.started.wait()
        if self.readiness_error is not None:
            raise self.readiness_error

    async def stop(self, *, force: "bool" = False) -> "bool":
        self.order.append(f"worker.stop:{force}")
        self.stop_calls.append(force)
        self.stop_entered.set()
        if force:
            if self.force_stop_error is not None:
                raise self.force_stop_error
            await self.allow_force_stop.wait()
            self.finish.set()
            return False
        await self.allow_graceful_stop.wait()
        if self.crash_during_graceful_stop:
            self.crash.set()
        else:
            self.finish.set()
        try:
            await self.allow_graceful_result.wait()
        except asyncio.CancelledError:
            self.graceful_stop_cancelled = True
            raise
        if self.graceful_stop_error is not None:
            raise self.graceful_stop_error
        return self.stop_result


class _ImmediateSuccessWorker:
    def __init__(self, order: "list[str]") -> "None":
        self.order = order
        self.stop_calls: "list[bool]" = []

    async def start(self) -> "None":
        self.order.append("worker.start")

    async def wait_started(self) -> "None":
        self.order.append("worker.wait_started")

    async def stop(self, *, force: "bool" = False) -> "bool":
        self.order.append(f"worker.stop:{force}")
        self.stop_calls.append(force)
        return False


def _worker_factory(worker: "_FakeWorker") -> "Callable[[QueueService, WorkerConfig], _FakeWorker]":
    def factory(service: "QueueService", config: "WorkerConfig") -> "_FakeWorker":
        del service, config
        return worker

    return factory


def _loader(order: "list[str]", error: "BaseException | None" = None) -> "Callable[[tuple[str, ...]], int]":
    def load(modules: "tuple[str, ...]") -> "int":
        assert modules == ("tests.fake_tasks",)
        order.append("tasks.load")
        if error is not None:
            raise error
        return 1

    return load


def _config(*, initialize_schedules: "bool" = True) -> "QueueConfig":
    return QueueConfig(
        worker=WorkerConfig(placement="external"),
        queue_backend="memory",
        task_modules=("tests.fake_tasks",),
        initialize_schedules=initialize_schedules,
    )


async def test_clean_graceful_stop_uses_exact_lifecycle_order() -> "None":
    order: "list[str]" = []
    service = _FakeService(order)
    worker = _FakeWorker(order)
    graceful_stop = asyncio.Event()
    ready_calls = 0

    def ready() -> "None":
        nonlocal ready_calls
        ready_calls += 1
        order.append("ready")
        graceful_stop.set()

    result = await run_worker(
        service,  # type: ignore[arg-type]
        _config(),
        graceful_stop=graceful_stop,
        force_stop=asyncio.Event(),
        ready=ready,
        _worker_factory=_worker_factory(worker),
        _task_loader=_loader(order),
    )

    assert result is WorkerRunResult.CLEAN
    assert ready_calls == 1
    assert service.schedule_calls == 1
    assert service.close_calls == 1
    assert order == [
        "tasks.load",
        "service.open",
        "service.initialize_schedules",
        "worker.start",
        "worker.wait_started",
        "ready",
        "worker.stop:False",
        "service.close",
    ]


async def test_worker_completion_before_stop_is_crashed_and_cleanup_error_does_not_mask_it() -> "None":
    order: "list[str]" = []
    close_error = RuntimeError("cleanup credential=close-secret")
    service = _FakeService(order, close_error=close_error)
    worker = _FakeWorker(order)

    def ready() -> "None":
        order.append("ready")
        worker.crash.set()

    result = await run_worker(
        service,  # type: ignore[arg-type]
        _config(initialize_schedules=False),
        graceful_stop=asyncio.Event(),
        force_stop=asyncio.Event(),
        ready=ready,
        _worker_factory=_worker_factory(worker),
        _task_loader=_loader(order),
    )

    assert result is WorkerRunResult.CRASHED
    assert worker.stop_calls == []
    assert service.close_calls == 1
    assert order == ["tasks.load", "service.open", "worker.start", "worker.wait_started", "ready", "service.close"]


@pytest.mark.parametrize(
    ("failure_point", "expected_stage"),
    [
        ("load", _WorkerStage.LOAD_TASKS),
        ("open", _WorkerStage.OPEN_SERVICE),
        ("schedules", _WorkerStage.INITIALIZE_SCHEDULES),
        ("readiness", _WorkerStage.START_WORKER),
        ("start", _WorkerStage.START_WORKER),
        ("factory", _WorkerStage.START_WORKER),
        ("ready_callback", _WorkerStage.START_WORKER),
    ],
)
async def test_startup_failures_map_to_exact_safe_stage(failure_point: "str", expected_stage: "_WorkerStage") -> "None":
    order: "list[str]" = []
    original = RuntimeError("credential=super-secret")
    service = _FakeService(
        order,
        open_error=original if failure_point == "open" else None,
        schedule_error=original if failure_point == "schedules" else None,
        close_error=RuntimeError("cleanup-secret") if failure_point == "schedules" else None,
    )
    worker = _FakeWorker(
        order,
        readiness_error=original if failure_point == "readiness" else None,
        start_error=original if failure_point == "start" else None,
    )

    def factory(service_: "QueueService", config: "WorkerConfig") -> "_FakeWorker":
        del service_, config
        if failure_point == "factory":
            raise original
        return worker

    def ready() -> "None":
        if failure_point == "ready_callback":
            raise original

    with pytest.raises(_WorkerStageError) as exc_info:
        await run_worker(
            service,  # type: ignore[arg-type]
            _config(),
            graceful_stop=asyncio.Event(),
            force_stop=asyncio.Event(),
            ready=ready,
            _worker_factory=factory,
            _task_loader=_loader(order, original if failure_point == "load" else None),
        )

    error = exc_info.value
    assert error.stage is expected_stage
    assert error.exception_type == "RuntimeError"
    assert error.__cause__ is original
    assert "super-secret" not in str(error)
    assert "super-secret" not in repr(error)
    assert set(vars(error)) == {"exception_type", "stage"}

    if failure_point == "load":
        assert order == ["tasks.load"]
        assert service.close_calls == 0
    elif failure_point == "open":
        assert order == ["tasks.load", "service.open"]
        assert service.close_calls == 0
    elif failure_point == "schedules":
        assert order == ["tasks.load", "service.open", "service.initialize_schedules", "service.close"]
        assert service.close_calls == 1
    else:
        assert order[-1] == "service.close"
        assert service.close_calls == 1


async def test_stop_before_readiness_stops_without_calling_ready() -> "None":
    order: "list[str]" = []
    service = _FakeService(order)
    worker = _FakeWorker(order, delay_readiness=True)
    graceful_stop = asyncio.Event()
    graceful_stop.set()
    ready_calls = 0

    def ready() -> "None":
        nonlocal ready_calls
        ready_calls += 1

    result = await run_worker(
        service,  # type: ignore[arg-type]
        _config(),
        graceful_stop=graceful_stop,
        force_stop=asyncio.Event(),
        ready=ready,
        _worker_factory=_worker_factory(worker),
        _task_loader=_loader(order),
    )

    assert result is WorkerRunResult.CLEAN
    assert ready_calls == 0
    assert worker.stop_calls == [False]
    assert service.close_calls == 1


async def test_preset_graceful_stop_does_not_hide_start_failure() -> "None":
    order: "list[str]" = []
    service = _FakeService(order)
    start_error = RuntimeError("start failed")
    worker = _FakeWorker(order, start_error=start_error)
    graceful_stop = asyncio.Event()
    graceful_stop.set()

    with pytest.raises(_WorkerStageError) as exc_info:
        await run_worker(
            service,  # type: ignore[arg-type]
            _config(),
            graceful_stop=graceful_stop,
            force_stop=asyncio.Event(),
            _worker_factory=_worker_factory(worker),
            _task_loader=_loader(order),
        )

    assert exc_info.value.stage is _WorkerStage.START_WORKER
    assert exc_info.value.__cause__ is start_error
    assert service.close_calls == 1


async def test_graceful_stop_tie_does_not_hide_readiness_failure() -> "None":
    order: "list[str]" = []
    service = _FakeService(order)
    readiness_error = RuntimeError("readiness failed")
    worker = _FakeWorker(order, readiness_error=readiness_error)
    graceful_stop = asyncio.Event()
    graceful_stop.set()
    ready_calls = 0

    def ready() -> "None":
        nonlocal ready_calls
        ready_calls += 1

    with pytest.raises(_WorkerStageError) as exc_info:
        await run_worker(
            service,  # type: ignore[arg-type]
            _config(),
            graceful_stop=graceful_stop,
            force_stop=asyncio.Event(),
            ready=ready,
            _worker_factory=_worker_factory(worker),
            _task_loader=_loader(order),
        )

    assert exc_info.value.stage is _WorkerStage.START_WORKER
    assert exc_info.value.__cause__ is readiness_error
    assert ready_calls == 0
    assert service.close_calls == 1


async def test_successful_readiness_and_worker_completion_tie_is_crashed() -> "None":
    order: "list[str]" = []
    service = _FakeService(order)
    worker = _ImmediateSuccessWorker(order)
    ready_calls = 0

    def ready() -> "None":
        nonlocal ready_calls
        ready_calls += 1

    result = await run_worker(
        service,  # type: ignore[arg-type]
        _config(),
        graceful_stop=asyncio.Event(),
        force_stop=asyncio.Event(),
        ready=ready,
        _worker_factory=_worker_factory(worker),  # type: ignore[arg-type]
        _task_loader=_loader(order),
    )

    assert result is WorkerRunResult.CRASHED
    assert ready_calls == 1
    assert worker.stop_calls == []
    assert service.close_calls == 1


@pytest.mark.parametrize(
    ("stop_kind", "expected", "expected_stop_calls"),
    [("force", WorkerRunResult.ESCALATED, [True]), ("graceful", WorkerRunResult.CLEAN, [False])],
)
async def test_preset_stop_after_successful_readiness_does_not_publish_ready(
    stop_kind: "str", expected: "WorkerRunResult", expected_stop_calls: "list[bool]"
) -> "None":
    order: "list[str]" = []
    service = _FakeService(order)
    worker = _ImmediateSuccessWorker(order)
    graceful_stop = asyncio.Event()
    force_stop = asyncio.Event()
    (force_stop if stop_kind == "force" else graceful_stop).set()
    ready_calls = 0

    def ready() -> "None":
        nonlocal ready_calls
        ready_calls += 1

    result = await run_worker(
        service,  # type: ignore[arg-type]
        _config(),
        graceful_stop=graceful_stop,
        force_stop=force_stop,
        ready=ready,
        _worker_factory=_worker_factory(worker),  # type: ignore[arg-type]
        _task_loader=_loader(order),
    )

    assert result is expected
    assert ready_calls == 0
    assert worker.stop_calls == expected_stop_calls
    assert service.close_calls == 1


async def test_worker_cancellation_after_ready_propagates() -> "None":
    order: "list[str]" = []
    service = _FakeService(order)
    cancellation = asyncio.CancelledError("post-readiness cancellation")
    worker = _FakeWorker(order, post_readiness_error=cancellation)
    ready_calls = 0

    def ready() -> "None":
        nonlocal ready_calls
        ready_calls += 1
        worker.allow_post_readiness_error.set()

    # Python 3.10 does not preserve the raised CancelledError instance or its
    # message across the await, so assert propagation and cleanup, not identity.
    with pytest.raises(asyncio.CancelledError):
        await run_worker(
            service,  # type: ignore[arg-type]
            _config(),
            graceful_stop=asyncio.Event(),
            force_stop=asyncio.Event(),
            ready=ready,
            _worker_factory=_worker_factory(worker),
            _task_loader=_loader(order),
        )

    assert ready_calls == 1
    assert worker.start_finished.is_set()
    assert service.close_calls == 1


async def test_force_before_graceful_uses_forced_stop_and_escalates() -> "None":
    order: "list[str]" = []
    service = _FakeService(order)
    worker = _FakeWorker(order)
    force_stop = asyncio.Event()

    def ready() -> "None":
        force_stop.set()

    result = await run_worker(
        service,  # type: ignore[arg-type]
        _config(),
        graceful_stop=asyncio.Event(),
        force_stop=force_stop,
        ready=ready,
        _worker_factory=_worker_factory(worker),
        _task_loader=_loader(order),
    )

    assert result is WorkerRunResult.ESCALATED
    assert worker.stop_calls == [True]
    assert service.close_calls == 1


async def test_force_during_graceful_drain_preempts_first_stop() -> "None":
    order: "list[str]" = []
    service = _FakeService(order)
    worker = _FakeWorker(order, delay_graceful_stop=True)
    graceful_stop = asyncio.Event()
    force_stop = asyncio.Event()

    def ready() -> "None":
        graceful_stop.set()

    runner = asyncio.create_task(
        run_worker(
            service,  # type: ignore[arg-type]
            _config(),
            graceful_stop=graceful_stop,
            force_stop=force_stop,
            ready=ready,
            _worker_factory=_worker_factory(worker),
            _task_loader=_loader(order),
        )
    )
    await asyncio.wait_for(worker.stop_entered.wait(), timeout=1)
    force_stop.set()

    assert await asyncio.wait_for(runner, timeout=1) is WorkerRunResult.ESCALATED
    assert worker.stop_calls == [False, True]
    assert service.close_calls == 1


async def test_worker_reported_drain_escalation_maps_to_escalated() -> "None":
    order: "list[str]" = []
    service = _FakeService(order)
    worker = _FakeWorker(order, stop_result=True)
    graceful_stop = asyncio.Event()

    def ready() -> "None":
        graceful_stop.set()

    result = await run_worker(
        service,  # type: ignore[arg-type]
        _config(),
        graceful_stop=graceful_stop,
        force_stop=asyncio.Event(),
        ready=ready,
        _worker_factory=_worker_factory(worker),
        _task_loader=_loader(order),
    )

    assert result is WorkerRunResult.ESCALATED
    assert worker.stop_calls == [False]
    assert service.close_calls == 1


@pytest.mark.parametrize(
    ("stop_result", "crash_during_stop", "expected"),
    [
        (True, False, WorkerRunResult.ESCALATED),
        (False, False, WorkerRunResult.CLEAN),
        (False, True, WorkerRunResult.CRASHED),
    ],
)
async def test_worker_completion_does_not_discard_pending_graceful_stop_result(
    stop_result: "bool", crash_during_stop: "bool", expected: "WorkerRunResult"
) -> "None":
    order: "list[str]" = []
    service = _FakeService(order)
    worker = _FakeWorker(
        order, stop_result=stop_result, delay_graceful_result=True, crash_during_graceful_stop=crash_during_stop
    )
    graceful_stop = asyncio.Event()

    def ready() -> "None":
        graceful_stop.set()

    runner = asyncio.create_task(
        run_worker(
            service,  # type: ignore[arg-type]
            _config(),
            graceful_stop=graceful_stop,
            force_stop=asyncio.Event(),
            ready=ready,
            _worker_factory=_worker_factory(worker),
            _task_loader=_loader(order),
        )
    )
    await asyncio.wait_for(worker.start_finished.wait(), timeout=1)
    await asyncio.sleep(0)
    worker.allow_graceful_result.set()

    assert await asyncio.wait_for(runner, timeout=1) is expected
    assert worker.graceful_stop_cancelled is False
    assert service.close_calls == 1


async def test_force_remains_live_after_worker_completes_during_graceful_stop() -> "None":
    order: "list[str]" = []
    service = _FakeService(order)
    worker = _FakeWorker(order, delay_graceful_result=True)
    graceful_stop = asyncio.Event()
    force_stop = asyncio.Event()

    def ready() -> "None":
        graceful_stop.set()

    runner = asyncio.create_task(
        run_worker(
            service,  # type: ignore[arg-type]
            _config(),
            graceful_stop=graceful_stop,
            force_stop=force_stop,
            ready=ready,
            _worker_factory=_worker_factory(worker),
            _task_loader=_loader(order),
        )
    )
    await asyncio.wait_for(worker.start_finished.wait(), timeout=1)
    await asyncio.sleep(0)
    force_stop.set()

    assert await asyncio.wait_for(runner, timeout=1) is WorkerRunResult.ESCALATED
    assert worker.graceful_stop_cancelled is True
    assert worker.stop_calls == [False, True]
    assert service.close_calls == 1


@pytest.mark.parametrize("control_flow", [asyncio.CancelledError(), KeyboardInterrupt(), SystemExit()])
async def test_startup_control_flow_is_not_wrapped(control_flow: "BaseException") -> "None":
    order: "list[str]" = []
    service = _FakeService(order)

    with pytest.raises(type(control_flow)) as exc_info:
        await run_worker(
            service,  # type: ignore[arg-type]
            _config(),
            graceful_stop=asyncio.Event(),
            force_stop=asyncio.Event(),
            _task_loader=_loader(order, control_flow),
        )

    assert exc_info.value is control_flow
    assert service.close_calls == 0


async def test_external_cancellation_during_pending_startup_propagates_and_cleans_up() -> "None":
    order: "list[str]" = []
    service = _FakeService(order)
    worker = _FakeWorker(order, delay_readiness=True)
    runner = asyncio.create_task(
        run_worker(
            service,  # type: ignore[arg-type]
            _config(),
            graceful_stop=asyncio.Event(),
            force_stop=asyncio.Event(),
            _worker_factory=_worker_factory(worker),
            _task_loader=_loader(order),
        )
    )
    await asyncio.wait_for(worker.wait_started_entered.wait(), timeout=1)
    runner.cancel()

    with pytest.raises(asyncio.CancelledError):
        await runner

    assert worker.start_finished.is_set()
    assert worker.stop_calls == [False]
    assert service.close_calls == 1


async def test_cancellation_from_pending_forced_stop_propagates() -> "None":
    order: "list[str]" = []
    service = _FakeService(order)
    worker = _FakeWorker(order, delay_force_stop=True)
    force_stop = asyncio.Event()

    def ready() -> "None":
        force_stop.set()

    runner = asyncio.create_task(
        run_worker(
            service,  # type: ignore[arg-type]
            _config(),
            graceful_stop=asyncio.Event(),
            force_stop=force_stop,
            ready=ready,
            _worker_factory=_worker_factory(worker),
            _task_loader=_loader(order),
        )
    )
    await asyncio.wait_for(worker.stop_entered.wait(), timeout=1)
    runner.cancel()

    with pytest.raises(asyncio.CancelledError):
        await runner

    assert worker.stop_calls == [True]
    assert worker.start_finished.is_set()
    assert service.close_calls == 1


async def test_service_close_control_flow_propagates_after_escalation() -> "None":
    order: "list[str]" = []
    cancellation = asyncio.CancelledError()
    service = _FakeService(order, close_error=cancellation)
    worker = _FakeWorker(order)
    force_stop = asyncio.Event()

    def ready() -> "None":
        force_stop.set()

    with pytest.raises(asyncio.CancelledError) as exc_info:
        await run_worker(
            service,  # type: ignore[arg-type]
            _config(),
            graceful_stop=asyncio.Event(),
            force_stop=force_stop,
            ready=ready,
            _worker_factory=_worker_factory(worker),
            _task_loader=_loader(order),
        )

    assert exc_info.value is cancellation
    assert worker.stop_calls == [True]
    assert service.close_calls == 1


async def test_service_close_error_is_raised_after_clean_stop() -> "None":
    order: "list[str]" = []
    close_error = RuntimeError("close failed")
    service = _FakeService(order, close_error=close_error)
    worker = _FakeWorker(order)
    graceful_stop = asyncio.Event()

    def ready() -> "None":
        graceful_stop.set()

    with pytest.raises(RuntimeError, match="close failed") as exc_info:
        await run_worker(
            service,  # type: ignore[arg-type]
            _config(),
            graceful_stop=graceful_stop,
            force_stop=asyncio.Event(),
            ready=ready,
            _worker_factory=_worker_factory(worker),
            _task_loader=_loader(order),
        )

    assert exc_info.value is close_error
    assert service.close_calls == 1


async def test_stage_and_result_values_are_exact() -> "None":
    assert [(member.name, member.value) for member in WorkerRunResult] == [
        ("CLEAN", 0),
        ("CRASHED", 1),
        ("ESCALATED", 2),
    ]
    assert [member.value for member in _WorkerStage] == [
        "load_tasks",
        "open_service",
        "initialize_schedules",
        "start_worker",
    ]


async def test_runner_does_not_leave_internal_tasks_pending() -> "None":
    order: "list[str]" = []
    service = _FakeService(order)
    worker = _FakeWorker(order)
    graceful_stop = asyncio.Event()
    before = set(asyncio.all_tasks())

    def ready() -> "None":
        graceful_stop.set()

    assert (
        await run_worker(
            service,  # type: ignore[arg-type]
            _config(),
            graceful_stop=graceful_stop,
            force_stop=asyncio.Event(),
            ready=ready,
            _worker_factory=_worker_factory(worker),
            _task_loader=_loader(order),
        )
        is WorkerRunResult.CLEAN
    )
    await asyncio.sleep(0)
    assert asyncio.all_tasks() <= before


def test_worker_runtime_import_does_not_load_click() -> "None":
    process = subprocess.run(
        [
            sys.executable,
            "-c",
            ("import sys; import litestar_queues.worker.runtime; raise SystemExit(1 if 'click' in sys.modules else 0)"),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert process.returncode == 0, process.stderr

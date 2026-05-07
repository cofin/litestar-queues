import asyncio
import inspect
import pkgutil
import random
import sys
import zoneinfo
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from importlib import import_module, reload
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast, overload

from typing_extensions import ParamSpec, Self

if TYPE_CHECKING:
    from types import ModuleType
    from uuid import UUID

    from litestar_queues.events import TaskExecutionContext
    from litestar_queues.models import QueuedTaskRecord, TaskStatus
    from litestar_queues.service import QueueService

__all__ = (
    "ScheduleConfig",
    "Task",
    "TaskResult",
    "clear_task_registry",
    "get_scheduled_tasks",
    "get_task_registry",
    "load_task_modules",
    "task",
)

P = ParamSpec("P")
T = TypeVar("T")
TaskCallable = Callable[P, T | Awaitable[T]]
AnyTaskCallable = Callable[..., Any]

_task_registry: dict[str, "Task[Any, Any]"] = {}
_schedule_registry: dict[str, "ScheduleConfig"] = {}
_loaded_modules: set[str] = set()


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _coerce_interval(value: float | timedelta | None) -> timedelta | None:
    if value is None:
        return None
    if isinstance(value, timedelta):
        return value
    return timedelta(seconds=value)


def _parse_cron_value(value: str, names: dict[str, int]) -> int:
    normalized = value.upper()
    if normalized in names:
        return names[normalized]
    return int(value)


def _expand_cron_field(
    field: str,
    *,
    minimum: int,
    maximum: int,
    names: dict[str, int] | None = None,
    normalize_sunday: bool = False,
) -> set[int]:
    names = names or {}
    values: set[int] = set()

    for raw_part in field.split(","):
        part = raw_part.strip()
        if not part:
            msg = "Cron fields cannot be empty"
            raise ValueError(msg)

        if "/" in part:
            range_part, step_part = part.split("/", 1)
            step = int(step_part)
            if step <= 0:
                msg = "Cron step values must be positive"
                raise ValueError(msg)
        else:
            range_part = part
            step = 1

        if range_part == "*":
            start = minimum
            end = maximum
        elif "-" in range_part:
            start_part, end_part = range_part.split("-", 1)
            start = _parse_cron_value(start_part, names)
            end = _parse_cron_value(end_part, names)
        else:
            start = _parse_cron_value(range_part, names)
            end = maximum if "/" in part else start

        if start > end and not normalize_sunday:
            msg = f"Invalid cron range: {raw_part}"
            raise ValueError(msg)

        if not minimum <= start <= maximum or not minimum <= end <= maximum:
            msg = f"Cron value out of range: {raw_part}"
            raise ValueError(msg)

        values.update(range(start, end + 1, step))

    if normalize_sunday and 7 in values:
        values.remove(7)
        values.add(0)
    return values


@dataclass(frozen=True, slots=True)
class ScheduleConfig:
    """Configuration for a recurring task schedule."""

    task_name: str
    cron: str | None = None
    interval: timedelta | int | float | None = None
    timezone: str = "UTC"
    initial_delay: timedelta | int | float = 0
    jitter: timedelta | int | float = 0
    max_instances: int = 1
    timeout: float | None = None

    def __post_init__(self) -> None:
        if self.cron is not None and self.interval is not None:
            msg = "Cannot specify both cron and interval"
            raise ValueError(msg)
        if self.cron is not None:
            self._parse_cron()
        object.__setattr__(self, "interval", _coerce_interval(self.interval))
        object.__setattr__(self, "initial_delay", _coerce_interval(self.initial_delay) or timedelta())
        object.__setattr__(self, "jitter", _coerce_interval(self.jitter) or timedelta())

    def get_next_run(self, after: datetime | None = None, *, use_initial_delay: bool = False) -> datetime:
        """Calculate the next scheduled run time.

        Returns:
            The next run time in UTC.

        Raises:
            ValueError: If no interval or cron expression is configured.
        """
        base = _ensure_utc(after or datetime.now(timezone.utc))
        initial_delay = cast("timedelta", self.initial_delay)
        interval = cast("timedelta | None", self.interval)

        if use_initial_delay and initial_delay:
            return self._apply_jitter(base + initial_delay)
        if interval is not None:
            return self._apply_jitter(base + interval)
        if self.cron is None:
            msg = "Schedule must have either cron or interval"
            raise ValueError(msg)
        return self._apply_jitter(self._get_next_cron_run(base))

    def as_metadata(self) -> dict[str, Any]:
        """Return a JSON-compatible metadata representation."""
        interval = cast("timedelta | None", self.interval)
        initial_delay = cast("timedelta", self.initial_delay)
        jitter = cast("timedelta", self.jitter)
        return {
            "cron": self.cron,
            "initial_delay": initial_delay.total_seconds(),
            "interval": interval.total_seconds() if interval is not None else None,
            "jitter": jitter.total_seconds(),
            "max_instances": self.max_instances,
            "task_name": self.task_name,
            "timeout": self.timeout,
            "timezone": self.timezone,
        }

    def _parse_cron(self) -> tuple[set[int], set[int], set[int], set[int], set[int]]:
        if self.cron is None:
            msg = "Cron expression is not configured"
            raise ValueError(msg)

        aliases = {
            "@annually": "0 0 1 1 *",
            "@daily": "0 0 * * *",
            "@hourly": "0 * * * *",
            "@midnight": "0 0 * * *",
            "@monthly": "0 0 1 * *",
            "@weekly": "0 0 * * 0",
            "@yearly": "0 0 1 1 *",
        }
        expression = aliases.get(self.cron, self.cron)
        parts = expression.split()
        if len(parts) != 5:
            msg = f"Invalid cron expression: {self.cron}"
            raise ValueError(msg)

        month_names = {
            "APR": 4,
            "AUG": 8,
            "DEC": 12,
            "FEB": 2,
            "JAN": 1,
            "JUL": 7,
            "JUN": 6,
            "MAR": 3,
            "MAY": 5,
            "NOV": 11,
            "OCT": 10,
            "SEP": 9,
        }
        weekday_names = {"FRI": 5, "MON": 1, "SAT": 6, "SUN": 0, "THU": 4, "TUE": 2, "WED": 3}
        try:
            return (
                _expand_cron_field(parts[0], minimum=0, maximum=59),
                _expand_cron_field(parts[1], minimum=0, maximum=23),
                _expand_cron_field(parts[2], minimum=1, maximum=31),
                _expand_cron_field(parts[3], minimum=1, maximum=12, names=month_names),
                _expand_cron_field(parts[4], minimum=0, maximum=7, names=weekday_names, normalize_sunday=True),
            )
        except (KeyError, TypeError, ValueError) as exc:
            msg = f"Invalid cron expression: {self.cron}"
            raise ValueError(msg) from exc

    def _get_next_cron_run(self, after: datetime) -> datetime:
        minutes, hours, days, months, weekdays = self._parse_cron()
        tz = zoneinfo.ZoneInfo(self.timezone)
        candidate = after.astimezone(tz).replace(second=0, microsecond=0) + timedelta(minutes=1)
        max_attempts = 366 * 24 * 60

        for _ in range(max_attempts):
            cron_weekday = (candidate.weekday() + 1) % 7
            if (
                candidate.minute in minutes
                and candidate.hour in hours
                and candidate.day in days
                and candidate.month in months
                and cron_weekday in weekdays
            ):
                return candidate.astimezone(timezone.utc)
            candidate += timedelta(minutes=1)

        msg = f"No matching run found for cron expression: {self.cron}"
        raise ValueError(msg)

    def _apply_jitter(self, value: datetime) -> datetime:
        jitter = cast("timedelta", self.jitter)
        jitter_seconds = jitter.total_seconds()
        if jitter_seconds <= 0:
            return value
        return value + timedelta(seconds=random.uniform(0, jitter_seconds))  # noqa: S311


class TaskResult:
    """Handle to a queued task result."""

    __slots__ = ("_cached_record", "_service", "_task_id", "_task_name")

    def __init__(
        self,
        task_id: "UUID",
        task_name: str,
        *,
        service: "QueueService | None" = None,
        record: "QueuedTaskRecord | None" = None,
    ) -> None:
        self._task_id = task_id
        self._task_name = task_name
        self._service = service
        self._cached_record = record

    @property
    def id(self) -> "UUID":
        """Return the queue record ID."""
        return self._task_id

    @property
    def task_name(self) -> str:
        """Return the registered task name."""
        return self._task_name

    @property
    def status(self) -> "TaskStatus | None":
        """Return the cached task status."""
        return self._cached_record.status if self._cached_record is not None else None

    @property
    def result(self) -> Any:
        """Return the cached task result."""
        return self._cached_record.result if self._cached_record is not None else None

    @property
    def error(self) -> str | None:
        """Return the cached task error."""
        return self._cached_record.error if self._cached_record is not None else None

    @property
    def record(self) -> "QueuedTaskRecord | None":
        """Return the cached queue record."""
        return self._cached_record

    async def refresh(self) -> Self:
        """Refresh this handle from its queue service.

        Returns:
            The refreshed result handle.

        Raises:
            RuntimeError: If the result has no associated service.
        """
        if self._service is None:
            msg = "TaskResult.refresh() requires an associated QueueService."
            raise RuntimeError(msg)
        self._cached_record = await self._service.get_task(self._task_id)
        return self

    async def wait(self, *, timeout: float | None = None, poll_interval: float = 0.1) -> Self:
        """Wait until the task reaches a terminal status.

        Returns:
            The completed result handle.

        Raises:
            TimeoutError: If the timeout elapses before a terminal status.
        """
        start = asyncio.get_running_loop().time()
        while self.status not in {"cancelled", "completed", "failed"}:
            await self.refresh()
            if self.status in {"cancelled", "completed", "failed"}:
                break
            if timeout is not None and asyncio.get_running_loop().time() - start >= timeout:
                msg = f"Task {self._task_id} did not complete within {timeout}s"
                raise TimeoutError(msg)
            await asyncio.sleep(poll_interval)
        return self


class Task(Generic[P, T]):
    """Registered task wrapper with direct call and enqueue APIs."""

    __slots__ = (
        "__dict__",
        "_description",
        "_execution_backend",
        "_execution_profile",
        "_func",
        "_key",
        "_log_level",
        "_name",
        "_priority",
        "_queue",
        "_quiet_success",
        "_retries",
        "_run_after",
        "_timeout",
    )

    def __init__(
        self,
        func: TaskCallable[P, T],
        *,
        name: str,
        queue: str = "default",
        priority: int = 0,
        retries: int = 0,
        timeout: float | None = None,
        execution_backend: str | None = None,
        execution_profile: str | None = None,
        key: str | None = None,
        run_after: float | timedelta | None = None,
        description: str | None = None,
        log_level: str | None = None,
        quiet_success: bool | None = None,
    ) -> None:
        self._func = func
        self._name = name
        self._queue = queue
        self._priority = priority
        self._retries = retries
        self._timeout = timeout
        self._execution_backend = execution_backend
        self._execution_profile = execution_profile
        self._key = key
        self._run_after = _coerce_interval(run_after)
        self._description = description
        self._log_level = log_level
        self._quiet_success = quiet_success
        self.__job_name__ = name

    @property
    def name(self) -> str:
        """Return the registered task name."""
        return self._name

    @property
    def queue(self) -> str:
        """Return the default queue name."""
        return self._queue

    @property
    def priority(self) -> int:
        """Return the default priority."""
        return self._priority

    @property
    def retries(self) -> int:
        """Return the maximum retry count."""
        return self._retries

    @property
    def timeout(self) -> float | None:
        """Return the execution timeout."""
        return self._timeout

    @property
    def execution_backend(self) -> str | None:
        """Return the task-specific execution backend override."""
        return self._execution_backend

    @property
    def execution_profile(self) -> str | None:
        """Return the task-specific execution profile override."""
        return self._execution_profile

    @property
    def key(self) -> str | None:
        """Return the default deduplication key."""
        return self._key

    @property
    def run_after(self) -> timedelta | None:
        """Return the relative delay for enqueue operations."""
        return self._run_after

    @property
    def description(self) -> str | None:
        """Return the task description metadata."""
        return self._description

    @property
    def log_level(self) -> str | None:
        """Return the task log level metadata."""
        return self._log_level

    @property
    def quiet_success(self) -> bool | None:
        """Return whether successful completion logging should be quiet."""
        return self._quiet_success

    @property
    def function(self) -> TaskCallable[P, T]:
        """Return the wrapped callable."""
        return self._func

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:
        """Execute the wrapped callable directly.

        Returns:
            The wrapped callable result.
        """
        result = self._func(*args, **kwargs)
        if inspect.isawaitable(result):
            return await result
        return result

    async def execute_record(
        self,
        record: "QueuedTaskRecord",
        *,
        task_context: "TaskExecutionContext | None" = None,
        extra_kwargs: "dict[str, Any] | None" = None,
    ) -> T:
        """Execute this task for a queued record in worker context.

        Returns:
            The wrapped callable result.
        """
        kwargs = dict(record.kwargs)
        if extra_kwargs:
            kwargs.update(extra_kwargs)
        if self._accepts_job_id():
            kwargs["_job_id"] = record.id
        if task_context is not None and self._accepts_task_context():
            kwargs["_task_context"] = task_context
        if inspect.iscoroutinefunction(self._func):
            coroutine_func = cast("Callable[..., Awaitable[T]]", self._func)
            return await coroutine_func(*record.args, **kwargs)
        sync_func = cast("Callable[..., T]", self._func)
        return await asyncio.to_thread(sync_func, *record.args, **kwargs)

    def metadata(self, values: dict[str, Any] | None = None) -> dict[str, Any]:
        """Return enqueue metadata for this task."""
        metadata = dict(values or {})
        if self._description is not None:
            metadata["description"] = self._description
        if self._log_level is not None:
            metadata["log_level"] = self._log_level
        if self._quiet_success is not None:
            metadata["quiet_success"] = self._quiet_success
        return metadata

    def _accepts_job_id(self) -> bool:
        signature = inspect.signature(self._func)
        parameters = signature.parameters
        return "_job_id" in parameters or any(
            param.kind == inspect.Parameter.VAR_KEYWORD for param in parameters.values()
        )

    def _accepts_task_context(self) -> bool:
        signature = inspect.signature(self._func)
        parameters = signature.parameters
        return "_task_context" in parameters or any(
            param.kind == inspect.Parameter.VAR_KEYWORD for param in parameters.values()
        )

    def using(
        self,
        *,
        queue: str | None = None,
        priority: int | None = None,
        retries: int | None = None,
        timeout: float | None = None,
        execution_backend: str | None = None,
        execution_profile: str | None = None,
        key: str | None = None,
        run_after: float | timedelta | None = None,
        description: str | None = None,
        log_level: str | None = None,
        quiet_success: bool | None = None,
    ) -> "Task[P, T]":
        """Return a configured copy with enqueue overrides."""
        return Task(
            self._func,
            name=self._name,
            queue=queue if queue is not None else self._queue,
            priority=priority if priority is not None else self._priority,
            retries=retries if retries is not None else self._retries,
            timeout=timeout if timeout is not None else self._timeout,
            execution_backend=execution_backend if execution_backend is not None else self._execution_backend,
            execution_profile=execution_profile if execution_profile is not None else self._execution_profile,
            key=key if key is not None else self._key,
            run_after=run_after if run_after is not None else self._run_after,
            description=description if description is not None else self._description,
            log_level=log_level if log_level is not None else self._log_level,
            quiet_success=quiet_success if quiet_success is not None else self._quiet_success,
        )

    async def enqueue(self, *args: P.args, **kwargs: P.kwargs) -> TaskResult:
        """Enqueue this task using a default immediate in-memory service.

        Returns:
            A result handle for the queued record.
        """
        from litestar_queues.config import QueueConfig
        from litestar_queues.service import QueueService

        enqueue_kwargs = cast("dict[str, Any]", kwargs)
        async with QueueService(QueueConfig(execution_backend="immediate")) as service:
            return await service.enqueue(cast("Task[Any, Any]", self), *args, **enqueue_kwargs)


def get_task_registry() -> dict[str, Task[Any, Any]]:
    """Return the global task registry."""
    return _task_registry


def get_scheduled_tasks() -> dict[str, ScheduleConfig]:
    """Return the global scheduled task registry."""
    return _schedule_registry


def clear_task_registry() -> None:
    """Clear task and schedule registries."""
    _task_registry.clear()
    _schedule_registry.clear()
    _loaded_modules.clear()


def load_task_modules(modules: tuple[str, ...] | list[str], *, force_reload: bool = False) -> int:
    """Import configured task modules so decorators register tasks.

    Returns:
        Number of imported modules.
    """
    loaded = 0
    for module_name in modules:
        if module_name in _loaded_modules and not force_reload:
            continue
        _loaded_modules.add(module_name)
        if force_reload or module_name in sys.modules:
            module = reload(sys.modules[module_name])
        else:
            module = import_module(module_name)
        loaded += 1
        loaded += _load_child_modules(module, force_reload=force_reload)
    return loaded


def _load_child_modules(module: "ModuleType", *, force_reload: bool) -> int:
    if not hasattr(module, "__path__"):
        return 0
    loaded = 0
    module_paths = module.__path__  # pyright: ignore[reportUnknownMemberType]
    for _, module_name, is_package in pkgutil.walk_packages(module_paths, prefix=f"{module.__name__}."):
        if is_package or (module_name in _loaded_modules and not force_reload):
            continue
        if force_reload and module_name in sys.modules:
            reload(sys.modules[module_name])
        else:
            import_module(module_name)
        _loaded_modules.add(module_name)
        loaded += 1
    return loaded


@overload
def task(func: Callable[P, Awaitable[T]], /) -> Task[P, T]: ...


@overload
def task(func: Callable[P, T], /) -> Task[P, T]: ...


@overload
def task(
    name: str | None = None,
    /,
    *,
    queue: str = "default",
    priority: int = 0,
    retries: int = 0,
    timeout: float | None = None,
    execution_backend: str | None = None,
    execution_profile: str | None = None,
    key: str | None = None,
    run_after: float | timedelta | None = None,
    description: str | None = None,
    log_level: str | None = None,
    quiet_success: bool | None = None,
    cron: str | None = None,
    interval: float | timedelta | None = None,
    timezone: str = "UTC",
    initial_delay: float | timedelta = 0,
    jitter: float | timedelta = 0,
    max_instances: int = 1,
) -> Callable[[AnyTaskCallable], Task[Any, Any]]: ...


def task(
    func_or_name: AnyTaskCallable | str | None = None,
    /,
    *,
    queue: str = "default",
    priority: int = 0,
    retries: int = 0,
    timeout: float | None = None,
    execution_backend: str | None = None,
    execution_profile: str | None = None,
    key: str | None = None,
    run_after: float | timedelta | None = None,
    description: str | None = None,
    log_level: str | None = None,
    quiet_success: bool | None = None,
    cron: str | None = None,
    interval: float | timedelta | None = None,
    timezone: str = "UTC",
    initial_delay: float | timedelta = 0,
    jitter: float | timedelta = 0,
    max_instances: int = 1,
) -> Task[Any, Any] | Callable[[AnyTaskCallable], Task[Any, Any]]:
    """Register a callable as a queue task.

    Returns:
        A task wrapper when used bare, otherwise a decorator.

    Raises:
        ValueError: If both cron and interval are configured.
    """
    if cron is not None and interval is not None:
        msg = "Cannot specify both cron and interval"
        raise ValueError(msg)

    explicit_name = func_or_name if isinstance(func_or_name, str) else None
    schedule = (
        ScheduleConfig(
            task_name=explicit_name or "",
            cron=cron,
            initial_delay=initial_delay,
            interval=interval,
            jitter=jitter,
            max_instances=max_instances,
            timeout=timeout,
            timezone=timezone,
        )
        if cron is not None or interval is not None
        else None
    )

    def decorator(func: AnyTaskCallable) -> Task[Any, Any]:
        task_name = explicit_name or func.__name__
        task_obj: Task[Any, Any] = Task(
            cast("TaskCallable[..., Any]", func),
            name=task_name,
            queue=queue,
            priority=priority,
            retries=retries,
            timeout=timeout,
            execution_backend=execution_backend,
            execution_profile=execution_profile,
            key=key,
            run_after=run_after,
            description=description,
            log_level=log_level,
            quiet_success=quiet_success,
        )
        _task_registry[task_name] = task_obj
        if schedule is not None:
            _schedule_registry[task_name] = ScheduleConfig(
                task_name=task_name,
                cron=schedule.cron,
                initial_delay=cast("timedelta", schedule.initial_delay),
                interval=cast("timedelta | None", schedule.interval),
                jitter=cast("timedelta", schedule.jitter),
                max_instances=schedule.max_instances,
                timeout=schedule.timeout,
                timezone=schedule.timezone,
            )
        return task_obj

    if callable(func_or_name) and not isinstance(func_or_name, str):
        return decorator(func_or_name)
    return decorator

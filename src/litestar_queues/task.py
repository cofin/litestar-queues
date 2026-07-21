import asyncio
import contextvars
import inspect
import pkgutil
import random
import sys
import zoneinfo
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import partial
from importlib import import_module, reload
from typing import TYPE_CHECKING, Any, Generic, NoReturn, TypeVar, cast, overload

from typing_extensions import ParamSpec, Self

if TYPE_CHECKING:
    from concurrent.futures import Executor
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
    "discover_tasks",
    "get_default_service",
    "get_scheduled_tasks",
    "get_task_registry",
    "load_task_modules",
    "set_default_service",
    "task",
)

P = ParamSpec("P")
T = TypeVar("T")
TaskCallable = Callable[P, T | Awaitable[T]]
AnyTaskCallable = Callable[..., Any]
StaleFailureHandler = Callable[["QueuedTaskRecord"], object | Awaitable[object]]

CRON_FIELD_COUNT = 5
CRON_SEARCH_YEARS = 8
SUNDAY_CRON_VALUE = 7

_task_registry: 'dict[str, "Task[Any, Any]"]' = {}
_schedule_registry: 'dict[str, "ScheduleConfig"]' = {}
_loaded_modules: "set[str]" = set()
_RANDOM = random.SystemRandom()
_default_service_holder: 'list["QueueService | None"]' = [None]


@dataclass(frozen=True, slots=True)
class _ParsedCron:
    minutes: "set[int]"
    hours: "set[int]"
    days: "set[int]"
    months: "set[int]"
    weekdays: "set[int]"
    day_of_month_restricted: "bool"
    day_of_week_restricted: "bool"


@dataclass(frozen=True, slots=True)
class ScheduleConfig:
    """Configuration for a recurring task schedule."""

    task_name: "str"
    cron: "str | None" = None
    interval: "timedelta | int | float | None" = None
    timezone: "str" = "UTC"
    initial_delay: "timedelta | int | float" = 0
    jitter: "timedelta | int | float" = 0
    max_instances: "int" = 1
    timeout: "float | None" = None

    def __post_init__(self) -> "None":
        if self.cron is not None and self.interval is not None:
            msg = "Cannot specify both cron and interval"
            raise ValueError(msg)
        interval = _coerce_interval(self.interval)
        initial_delay = _coerce_interval(self.initial_delay) or timedelta()
        jitter = _coerce_interval(self.jitter) or timedelta()
        if interval is not None and interval <= timedelta():
            msg = "Schedule interval must be positive"
            raise ValueError(msg)
        if initial_delay < timedelta():
            msg = "Schedule initial_delay cannot be negative"
            raise ValueError(msg)
        if jitter < timedelta():
            msg = "Schedule jitter cannot be negative"
            raise ValueError(msg)
        _get_timezone(self.timezone)
        if self.cron is not None:
            self._parse_cron()
        object.__setattr__(self, "interval", interval)
        object.__setattr__(self, "initial_delay", initial_delay)
        object.__setattr__(self, "jitter", jitter)

    def get_next_run(self, after: "datetime | None" = None, *, use_initial_delay: "bool" = False) -> "datetime":
        """Calculate the next scheduled run time.

        Returns:
            The next run time in UTC.

        Raises:
            ValueError: If no interval or cron expression is configured.
        """
        base = _ensure_utc(after or datetime.now(timezone.utc))
        initial_delay = self._initial_delay_value
        interval = self._interval_value

        if use_initial_delay and initial_delay:
            return self._apply_jitter(base + initial_delay)
        if interval is not None:
            return self._apply_jitter(base + interval)
        if self.cron is None:
            msg = "Schedule must have either cron or interval"
            raise ValueError(msg)
        return self._apply_jitter(self._get_next_cron_run(base))

    def as_metadata(self) -> "dict[str, Any]":
        """Return a JSON-compatible metadata representation."""
        return {
            "cron": self.cron,
            "initial_delay": self._initial_delay_value.total_seconds(),
            "interval": self._interval_value.total_seconds() if self._interval_value is not None else None,
            "jitter": self._jitter_value.total_seconds(),
            "max_instances": self.max_instances,
            "task_name": self.task_name,
            "timeout": self.timeout,
            "timezone": self.timezone,
        }

    def copy_for_task(self, task_name: "str") -> "ScheduleConfig":
        """Return this normalized schedule for another task name."""
        return ScheduleConfig(
            task_name=task_name,
            cron=self.cron,
            initial_delay=self._initial_delay_value,
            interval=self._interval_value,
            jitter=self._jitter_value,
            max_instances=self.max_instances,
            timeout=self.timeout,
            timezone=self.timezone,
        )

    def _parse_cron(self) -> "_ParsedCron":
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
        _raise_for_unsupported_cron_syntax(self.cron, expression)
        parts = expression.split()
        if len(parts) != CRON_FIELD_COUNT:
            msg = f"Invalid cron expression: {self.cron}"
            raise ValueError(msg)
        day_field = parts[2]
        weekday_field = parts[4]
        if day_field == "?" and weekday_field == "?":
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
            return _ParsedCron(
                minutes=_expand_cron_field(parts[0], minimum=0, maximum=59),
                hours=_expand_cron_field(parts[1], minimum=0, maximum=23),
                days=_expand_cron_field(day_field, minimum=1, maximum=31, allow_question=True),
                months=_expand_cron_field(parts[3], minimum=1, maximum=12, names=month_names),
                weekdays=_expand_cron_field(
                    weekday_field, minimum=0, maximum=7, names=weekday_names, normalize_sunday=True, allow_question=True
                ),
                day_of_month_restricted=day_field not in {"*", "?"},
                day_of_week_restricted=weekday_field not in {"*", "?"},
            )
        except (KeyError, TypeError, ValueError) as exc:
            msg = f"Invalid cron expression: {self.cron}"
            raise ValueError(msg) from exc

    def _get_next_cron_run(self, after: "datetime") -> "datetime":
        parsed = self._parse_cron()
        tz = _get_timezone(self.timezone)
        candidate = after.astimezone(tz).replace(second=0, microsecond=0) + timedelta(minutes=1)
        search_end = candidate + timedelta(days=CRON_SEARCH_YEARS * 366)
        day = candidate.replace(hour=0, minute=0, second=0, microsecond=0)
        time_slots = tuple((hour, minute) for hour in sorted(parsed.hours) for minute in sorted(parsed.minutes))

        while day <= search_end:
            cron_weekday = (day.weekday() + 1) % 7
            if day.month in parsed.months and _cron_day_matches(parsed, day=day.day, weekday=cron_weekday):
                for hour, minute in time_slots:
                    proposed = day.replace(hour=hour, minute=minute)
                    if proposed < candidate or not _is_valid_local_time(proposed, tz):
                        continue
                    return proposed.astimezone(timezone.utc)
            day = (day + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

        msg = f"No matching run found for cron expression: {self.cron}"
        raise ValueError(msg)

    def _apply_jitter(self, value: "datetime") -> "datetime":
        jitter = self._jitter_value
        jitter_seconds = jitter.total_seconds()
        if jitter_seconds <= 0:
            return value
        return value + timedelta(seconds=_RANDOM.uniform(0, jitter_seconds))

    @property
    def _initial_delay_value(self) -> "timedelta":
        value = self.initial_delay
        if isinstance(value, timedelta):
            return value
        return _coerce_interval(value) or timedelta()

    @property
    def _interval_value(self) -> "timedelta | None":
        value = self.interval
        if value is None or isinstance(value, timedelta):
            return value
        return _coerce_interval(value)

    @property
    def _jitter_value(self) -> "timedelta":
        value = self.jitter
        if isinstance(value, timedelta):
            return value
        return _coerce_interval(value) or timedelta()


class TaskResult:
    """Handle to a queued task result."""

    __slots__ = ("_cached_record", "_service", "_task_id", "_task_name")

    def __init__(
        self,
        task_id: "UUID",
        task_name: "str",
        *,
        service: "QueueService | None" = None,
        record: "QueuedTaskRecord | None" = None,
    ) -> "None":
        self._task_id = task_id
        self._task_name = task_name
        self._service = service
        self._cached_record = record

    @property
    def id(self) -> "UUID":
        """Queue record ID."""
        return self._task_id

    @property
    def task_name(self) -> "str":
        """Registered task name."""
        return self._task_name

    @property
    def status(self) -> "TaskStatus | None":
        """Cached task status."""
        return self._cached_record.status if self._cached_record is not None else None

    @property
    def result(self) -> "Any":
        """Cached task result."""
        return self._cached_record.result if self._cached_record is not None else None

    @property
    def error(self) -> "str | None":
        """Cached task error."""
        return self._cached_record.error if self._cached_record is not None else None

    @property
    def record(self) -> "QueuedTaskRecord | None":
        """Cached queue record."""
        return self._cached_record

    async def refresh(self) -> "Self":
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

    async def wait(self, *, timeout: "float | None" = None, poll_interval: "float" = 0.1) -> "Self":
        """Wait until the task reaches a terminal status.

        Returns:
            The completed result handle.

        Raises:
            TimeoutError: If the timeout elapses before a terminal status.
            RuntimeError: If the task no longer exists in the queue backend.
        """
        terminal = {"cancelled", "completed", "failed"}
        start = asyncio.get_running_loop().time()
        backend = self._service.get_queue_backend() if self._service is not None else None
        push = backend is not None and backend.capabilities.supports_completion_events
        while self.status not in terminal:
            await self.refresh()
            if self.record is None:
                msg = f"Task {self._task_id} no longer exists in the queue backend."
                raise RuntimeError(msg)
            if self.status in terminal:
                break
            if timeout is not None and asyncio.get_running_loop().time() - start >= timeout:
                msg = f"Task {self._task_id} did not complete within {timeout}s"
                raise TimeoutError(msg)
            wait_for = poll_interval
            if timeout is not None:
                wait_for = min(poll_interval, max(0.0, timeout - (asyncio.get_running_loop().time() - start)))
            if push and backend is not None:
                await backend.wait_for_completion(self._task_id, timeout=wait_for)
            else:
                await asyncio.sleep(wait_for)
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
        "_on_stale_failure",
        "_priority",
        "_queue",
        "_quiet_success",
        "_requeue_on_stale",
        "_retries",
        "_run_after",
        "_timeout",
    )

    def __init__(
        self,
        func: "TaskCallable[P, T]",
        *,
        name: "str",
        queue: "str" = "default",
        priority: "int" = 0,
        retries: "int" = 0,
        timeout: "float | None" = None,
        execution_backend: "str | None" = None,
        execution_profile: "str | None" = None,
        key: "str | None" = None,
        run_after: "float | timedelta | None" = None,
        description: "str | None" = None,
        log_level: "str | None" = None,
        quiet_success: "bool | None" = None,
        requeue_on_stale: "bool | None" = None,
        on_stale_failure: "StaleFailureHandler | None" = None,
    ) -> "None":
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
        self._requeue_on_stale = requeue_on_stale
        self._on_stale_failure = on_stale_failure

    @property
    def name(self) -> "str":
        """Registered task name."""
        return self._name

    @property
    def queue(self) -> "str":
        """Default queue name."""
        return self._queue

    @property
    def priority(self) -> "int":
        """Default priority."""
        return self._priority

    @property
    def retries(self) -> "int":
        """Maximum retry count."""
        return self._retries

    @property
    def timeout(self) -> "float | None":
        """Execution timeout."""
        return self._timeout

    @property
    def execution_backend(self) -> "str | None":
        """Task-specific execution backend override."""
        return self._execution_backend

    @property
    def execution_profile(self) -> "str | None":
        """Task-specific execution profile override."""
        return self._execution_profile

    @property
    def key(self) -> "str | None":
        """Default deduplication key."""
        return self._key

    @property
    def run_after(self) -> "timedelta | None":
        """Relative delay for enqueue operations."""
        return self._run_after

    @property
    def description(self) -> "str | None":
        """Task description metadata."""
        return self._description

    @property
    def log_level(self) -> "str | None":
        """Task log level metadata."""
        return self._log_level

    @property
    def quiet_success(self) -> "bool | None":
        """Whether successful completion logging should be quiet."""
        return self._quiet_success

    @property
    def requeue_on_stale(self) -> "bool":
        """Whether stale running records should be requeued when retries remain."""
        return self._requeue_on_stale is not False

    @property
    def on_stale_failure(self) -> "StaleFailureHandler | None":
        """Callback invoked after this task reaches terminal stale failure."""
        return self._on_stale_failure

    @property
    def function(self) -> "TaskCallable[P, T]":
        """Wrapped callable."""
        return self._func

    async def __call__(self, *args: "P.args", **kwargs: "P.kwargs") -> "T":
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
        extra_kwargs: "Mapping[str, object] | None" = None,
        sync_executor: "Executor | None" = None,
    ) -> "T":
        """Execute this task for a queued record in worker context.

        Returns:
            The wrapped callable result.
        """
        kwargs = dict(record.kwargs)
        if extra_kwargs:
            kwargs.update(extra_kwargs)
        if task_context is not None and self._accepts_task_context():
            kwargs["_task_context"] = task_context
        if inspect.iscoroutinefunction(self._func):
            coroutine_func = cast("Callable[..., Awaitable[T]]", self._func)
            return await coroutine_func(*record.args, **kwargs)
        sync_func = cast("Callable[..., T]", self._func)
        return await _run_sync_callable(sync_func, record.args, kwargs, sync_executor=sync_executor)

    def metadata(self, values: "dict[str, Any] | None" = None) -> "dict[str, Any]":
        """Return enqueue metadata for this task."""
        metadata = dict(values or {})
        if self._description is not None:
            metadata["description"] = self._description
        if self._log_level is not None:
            metadata["log_level"] = self._log_level
        if self._quiet_success is not None:
            metadata["quiet_success"] = self._quiet_success
        if self._requeue_on_stale is not None:
            metadata["requeue_on_stale"] = self._requeue_on_stale
        return metadata

    def using(
        self,
        *,
        queue: "str | None" = None,
        priority: "int | None" = None,
        retries: "int | None" = None,
        timeout: "float | None" = None,
        execution_backend: "str | None" = None,
        execution_profile: "str | None" = None,
        key: "str | None" = None,
        run_after: "float | timedelta | None" = None,
        description: "str | None" = None,
        log_level: "str | None" = None,
        quiet_success: "bool | None" = None,
        requeue_on_stale: "bool | None" = None,
        on_stale_failure: "StaleFailureHandler | None" = None,
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
            requeue_on_stale=requeue_on_stale if requeue_on_stale is not None else self._requeue_on_stale,
            on_stale_failure=on_stale_failure if on_stale_failure is not None else self._on_stale_failure,
        )

    async def enqueue(self, *args: "P.args", **kwargs: "P.kwargs") -> "TaskResult":
        """Enqueue this task using the configured default service or fall back to an immediate service.

        Returns:
            A result handle for the queued record.
        """
        enqueue_kwargs = cast("dict[str, Any]", kwargs)
        service = get_default_service()
        if service is not None:
            return await service.enqueue(cast("Task[Any, Any]", self), *args, **enqueue_kwargs)

        from litestar_queues.config import QueueConfig
        from litestar_queues.service import QueueService

        async with QueueService(QueueConfig(execution_backend="immediate")) as service:
            return await service.enqueue(cast("Task[Any, Any]", self), *args, **enqueue_kwargs)

    def _accepts_task_context(self) -> "bool":
        signature = inspect.signature(self._func)
        parameters = signature.parameters
        return "_task_context" in parameters or any(
            param.kind == inspect.Parameter.VAR_KEYWORD for param in parameters.values()
        )


def get_task_registry() -> "dict[str, Task[Any, Any]]":
    """Return the global task registry."""
    return _task_registry


def get_scheduled_tasks() -> "dict[str, ScheduleConfig]":
    """Return the global scheduled task registry."""
    return _schedule_registry


def get_default_service() -> "QueueService | None":
    """Return the global default QueueService instance."""
    return _default_service_holder[0]


def set_default_service(service: "QueueService | None") -> "None":
    """Set the global default QueueService instance."""
    _default_service_holder[0] = service


def clear_task_registry() -> "None":
    """Clear task and schedule registries."""
    _task_registry.clear()
    _schedule_registry.clear()
    _loaded_modules.clear()
    _default_service_holder[0] = None


def load_task_modules(modules: "tuple[str, ...] | list[str]", *, force_reload: "bool" = False) -> "int":
    """Import configured task modules so decorators register tasks.

    Returns:
        Number of imported modules.
    """
    loaded = 0
    for module_name in modules:
        if module_name in _loaded_modules and not force_reload:
            continue
        if force_reload and module_name in sys.modules:
            module = reload(sys.modules[module_name])
        else:
            module = import_module(module_name)
        child_count = _load_child_modules(module, force_reload=force_reload)
        _loaded_modules.add(module_name)
        loaded += 1 + child_count
    return loaded


def discover_tasks(package: "str", subpackage: "str" = "jobs", *, force_reload: "bool" = False) -> "tuple[str, ...]":
    """Walk ``package`` and import every ``<package>.<...>.<subpackage>.<...>`` module.

    Adopters with ``app.domain.<x>.jobs/`` layouts can call this once at
    startup so ``@task``-decorated callables register without having to
    enumerate ``QueueConfig.task_modules`` by hand.

    Args:
        package: Dotted package name to walk (e.g. ``"app.domain"``).
        subpackage: Path segment that marks task modules. Any module whose
            dotted path (excluding the root) contains this segment is
            imported. Defaults to ``"jobs"``.
        force_reload: Re-import modules already in ``sys.modules``.

    Returns:
        Sorted, deduplicated tuple of task names registered after the walk.

    Raises:
        ModuleNotFoundError: If ``package`` cannot be imported, or if it
            resolves to a plain module rather than a package.
    """
    root = reload(sys.modules[package]) if force_reload and package in sys.modules else import_module(package)
    if not hasattr(root, "__path__"):
        msg = f"discover_tasks requires a package; {package!r} is a module"
        raise ModuleNotFoundError(msg)

    matched: "list[str]" = []
    root_path = root.__dict__["__path__"]
    root_prefix = f"{root.__name__}."
    for _, module_name, _is_package in pkgutil.walk_packages(
        root_path, prefix=root_prefix, onerror=_raise_walk_packages_error
    ):
        relative_name = module_name.removeprefix(root_prefix)
        if subpackage not in relative_name.split("."):
            continue
        matched.append(module_name)

    for module_name in matched:
        if module_name in _loaded_modules and not force_reload:
            continue
        if force_reload and module_name in sys.modules:
            reload(sys.modules[module_name])
        else:
            import_module(module_name)
        _loaded_modules.add(module_name)

    return tuple(sorted(_task_registry.keys()))


@overload
def task(func: "Callable[P, Awaitable[T]]", /) -> "Task[P, T]": ...


@overload
def task(func: "Callable[P, T]", /) -> "Task[P, T]": ...


@overload
def task(
    name: "str | None" = None,
    /,
    *,
    queue: "str" = "default",
    priority: "int" = 0,
    retries: "int" = 0,
    timeout: "float | None" = None,
    execution_backend: "str | None" = None,
    execution_profile: "str | None" = None,
    key: "str | None" = None,
    run_after: "float | timedelta | None" = None,
    description: "str | None" = None,
    log_level: "str | None" = None,
    quiet_success: "bool | None" = None,
    requeue_on_stale: "bool | None" = None,
    on_stale_failure: "StaleFailureHandler | None" = None,
    cron: "str | None" = None,
    interval: "float | timedelta | None" = None,
    timezone: "str" = "UTC",
    initial_delay: "float | timedelta" = 0,
    jitter: "float | timedelta" = 0,
    max_instances: "int" = 1,
) -> "Callable[[AnyTaskCallable], Task[Any, Any]]": ...


def task(
    func_or_name: "AnyTaskCallable | str | None" = None,
    /,
    *,
    queue: "str" = "default",
    priority: "int" = 0,
    retries: "int" = 0,
    timeout: "float | None" = None,
    execution_backend: "str | None" = None,
    execution_profile: "str | None" = None,
    key: "str | None" = None,
    run_after: "float | timedelta | None" = None,
    description: "str | None" = None,
    log_level: "str | None" = None,
    quiet_success: "bool | None" = None,
    requeue_on_stale: "bool | None" = None,
    on_stale_failure: "StaleFailureHandler | None" = None,
    cron: "str | None" = None,
    interval: "float | timedelta | None" = None,
    timezone: "str" = "UTC",
    initial_delay: "float | timedelta" = 0,
    jitter: "float | timedelta" = 0,
    max_instances: "int" = 1,
) -> "Task[Any, Any] | Callable[[AnyTaskCallable], Task[Any, Any]]":
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

    def decorator(func: "AnyTaskCallable") -> "Task[Any, Any]":
        task_name = explicit_name or func.__name__
        task_obj: "Task[Any, Any]" = Task(
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
            requeue_on_stale=requeue_on_stale,
            on_stale_failure=on_stale_failure,
        )
        _task_registry[task_name] = task_obj
        if schedule is not None:
            _schedule_registry[task_name] = schedule.copy_for_task(task_name)
        return task_obj

    if callable(func_or_name) and not isinstance(func_or_name, str):
        return decorator(func_or_name)
    return decorator


def _ensure_utc(value: "datetime") -> "datetime":
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _coerce_interval(value: "float | timedelta | None") -> "timedelta | None":
    if value is None:
        return None
    if isinstance(value, timedelta):
        return value
    return timedelta(seconds=value)


def _get_timezone(name: "str") -> "zoneinfo.ZoneInfo":
    try:
        return zoneinfo.ZoneInfo(name)
    except zoneinfo.ZoneInfoNotFoundError as exc:
        msg = f"Invalid timezone: {name}"
        raise ValueError(msg) from exc


def _cron_day_matches(parsed: "_ParsedCron", *, day: "int", weekday: "int") -> "bool":
    day_matches = day in parsed.days
    weekday_matches = weekday in parsed.weekdays
    if parsed.day_of_month_restricted and parsed.day_of_week_restricted:
        return day_matches or weekday_matches
    return day_matches and weekday_matches


def _is_valid_local_time(value: "datetime", tz: "zoneinfo.ZoneInfo") -> "bool":
    round_tripped = value.astimezone(timezone.utc).astimezone(tz)
    return (
        round_tripped.year == value.year
        and round_tripped.month == value.month
        and round_tripped.day == value.day
        and round_tripped.hour == value.hour
        and round_tripped.minute == value.minute
        and round_tripped.second == value.second
        and round_tripped.microsecond == value.microsecond
        and round_tripped.fold == value.fold
        and round_tripped.utcoffset() == value.utcoffset()
    )


async def _run_sync_callable(
    func: "Callable[..., T]", args: "tuple[Any, ...]", kwargs: "dict[str, Any]", *, sync_executor: "Executor | None"
) -> "T":
    if sync_executor is None:
        return await asyncio.to_thread(func, *args, **kwargs)
    context = contextvars.copy_context()
    call = partial(context.run, func, *args, **kwargs)
    return await asyncio.get_running_loop().run_in_executor(sync_executor, call)


def _parse_cron_value(value: "str", names: "dict[str, int]") -> "int":
    normalized = value.upper()
    if normalized in names:
        return names[normalized]
    return int(value)


def _expand_cron_field(
    field: "str",
    *,
    minimum: "int",
    maximum: "int",
    names: "dict[str, int] | None" = None,
    normalize_sunday: "bool" = False,
    allow_question: "bool" = False,
) -> "set[int]":
    names = names or {}
    if allow_question and field == "?":
        return set(range(minimum, maximum + 1))

    values: "set[int]" = set()

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

        if start > end:
            msg = f"Invalid cron range: {raw_part}"
            raise ValueError(msg)

        if not minimum <= start <= maximum or not minimum <= end <= maximum:
            msg = f"Cron value out of range: {raw_part}"
            raise ValueError(msg)

        values.update(range(start, end + 1, step))

    if normalize_sunday and SUNDAY_CRON_VALUE in values:
        values.remove(SUNDAY_CRON_VALUE)
        values.add(0)
    return values


def _raise_for_unsupported_cron_syntax(original: "str", expression: "str") -> "None":
    parts = expression.split()
    unsupported: "list[str]" = []
    if original == "@reboot":
        unsupported.append("@reboot")
    if len(parts) > CRON_FIELD_COUNT:
        unsupported.append("year fields")
    if any("L" in part.upper() for part in parts):
        unsupported.append("L")
    if any("W" in part.upper() for part in parts):
        unsupported.append("W")
    if any("#" in part for part in parts):
        unsupported.append("#")
    if not unsupported:
        return
    unsupported_text = ", ".join(dict.fromkeys(unsupported))
    msg = (
        f"Unsupported cron syntax in {original!r}: {unsupported_text}. "
        "Use five-field POSIX cron, aliases other than @reboot, ranges, lists, steps, month/day names, or '?'."
    )
    raise ValueError(msg)


def _load_child_modules(module: "ModuleType", *, force_reload: "bool") -> "int":
    if not hasattr(module, "__path__"):
        return 0
    loaded = 0
    module_paths = module.__dict__["__path__"]
    for _, module_name, is_package in pkgutil.walk_packages(
        module_paths, prefix=f"{module.__name__}.", onerror=_raise_walk_packages_error
    ):
        if is_package or (module_name in _loaded_modules and not force_reload):
            continue
        if force_reload and module_name in sys.modules:
            reload(sys.modules[module_name])
        else:
            import_module(module_name)
        _loaded_modules.add(module_name)
        loaded += 1
    return loaded


def _raise_walk_packages_error(module_name: "str") -> "NoReturn":
    msg = f"Failed to import package {module_name!r} while discovering queue tasks"
    exc = sys.exc_info()[1]
    if exc is not None:
        raise ModuleNotFoundError(msg) from exc
    raise ModuleNotFoundError(msg)

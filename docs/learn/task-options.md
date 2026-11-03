# Task Options & Uniqueness

Every `@task` declaration sets default scheduling, timeout, thread-offloading, and deduplication behavior. Callers can override scheduling and identity options per invocation in `QueueService.enqueue(...)`.

--8<-- "tools/docs/includes/diagram-task-lifecycle.html"

## Runnable Example

```python title="docs/examples/learn/task_options_and_uniqueness.py"
--8<-- "docs/examples/learn/task_options_and_uniqueness.py"
```

## Priority & Delayed Execution

- **Priority (`priority: int = 0`)**: Higher integers are claimed before lower integers within the same due window (`priority=10` runs before `priority=0`).
- **Relative Delay (`run_after: float | timedelta | None`)**: Defers initial eligibility by adding the duration to the enqueue timestamp and storing the record with `status="scheduled"`.
- **Absolute Schedule (`scheduled_at: datetime | None`)**: Passed at `enqueue(..., scheduled_at=...)` to schedule a task for a specific UTC timestamp. Naive `datetime` objects are interpreted as UTC.

## Lifecycle Clocks

`litestar-queues` separates queue waiting deadlines, execution attempt limits, and post-terminal storage retention into independent clocks:

| Clock | Configuration | Active State | Expiration Outcome |
| :--- | :--- | :--- | :--- |
| **Queue Expiration Deadline** | `expires_in` / `expires_at` on `@task` or `enqueue()` | `pending` or `scheduled` (before claim) | Transitions unstarted task to `expired` and emits `task.expired` |
| **Attempt Execution Timeout** | `timeout` on `@task` or `enqueue()` | `running` (wraps dependency provider + task body) | Cancels running coroutine with `TimeoutError` and applies retry policy |
| **Terminal Retention TTL** | `terminal_ttl` on backend config / `WorkerConfig.terminal_retention` | `completed`, `failed`, `cancelled`, `expired` | Purges finished `QueuedTaskRecord` rows without touching `TaskReservation` |

!!! note "Expiration Evaluates Relative to Scheduled Time"
    When both `run_after` (or `scheduled_at`) and `expires_in` are set, `expires_at` is computed as `scheduled_at + expires_in`, giving a delayed task its full expiration window once it becomes due.

## Synchronous Callables & Thread Offloading

`litestar-queues` runs on an asyncio event loop. Synchronous `def` task functions are offloaded to a managed `ThreadPoolExecutor` (`QueueConfig.sync_thread_pool_size`, defaulting to `min(32, effective_cpu_count + 4)`).

| Function Kind | `sync_to_thread` Setting | Runtime Behavior |
| :--- | :--- | :--- |
| `async def` | `None` or `False` | Awaited directly on the worker event loop |
| `async def` | `True` | Rejected with `QueueConfigurationError` at registration |
| `def` (sync) | `None` (default) | Offloaded to the `QueueService` thread pool and emits `QueueWarning` recommending explicit `sync_to_thread=True` |
| `def` (sync) | `True` | Offloaded to the `QueueService` thread pool without warning |
| `def` (sync) | `False` | Executed inline on the event loop thread (use only for fast, non-blocking CPU work) |

## Task Identity & Deduplication

Deduplication prevents duplicate queue records from piling up when producers submit redundant work.

### Identity Precedence Order

When `QueueService.enqueue()` resolves the effective identity key stored on `QueuedTaskRecord.key`, it evaluates sources in strict precedence order:

| Precedence | Source | Key Format | Description |
| :--- | :--- | :--- | :--- |
| Highest | `enqueue(..., key="...")` | Caller string | Explicit per-call deduplication key |
| Second | `@task(..., key="...")` | Configured string | Static key declared on the task definition |
| Third | `@task(..., unique_by="task")` | `lq:u:v1:task:<sha256>` | At most one active instance of the task name regardless of arguments |
| Fourth | `@task(..., unique_by="arguments")` | `lq:u:v1:args:<sha256>` | Canonical JSON hash of `(task_name, bound_args, bound_kwargs)` |
| Lowest | None configured | `None` | Every `enqueue()` creates a distinct `QueuedTaskRecord` |

When `unique_by="arguments"` is active, `litestar-queues` binds positional and keyword arguments against the callable's `inspect.Signature`, applies default parameter values, normalizes collections, and hashes the serialized payload. Payloads exceeding `QueueConfig.max_argument_identity_bytes` (default `65536` bytes) raise `TaskIdentityTooLargeError`.

### Identity Lifetime: Terminal vs Forever

`unique_until` controls how long an identity key blocks subsequent `enqueue()` calls:

- **`unique_until="terminal"` (default)**: Blocks duplicate enqueues only while the existing record is non-terminal (`pending`, `scheduled`, or `running`). Matching `enqueue()` calls return a `TaskResult` pointing to the active record. Once the record reaches `completed`, `failed`, `cancelled`, or `expired`, the identity is automatically released.
- **`unique_until="forever"`**: Persists a lightweight [`TaskReservation`](../reference/tasks-and-models.md#litestar_queues.models.TaskReservation) (`key`, `task_id`, `task_name`, `created_at`) that survives terminal completion and routine `cleanup_completed` / `terminal_ttl` purges. Subsequent enqueues with the same identity return the existing reservation's task handle until an operator explicitly resets the key.

### Inspecting and Resetting Forever Reservations

Use `QueueService.get_task_identity(key)` and `QueueService.reset_task_identity(key)` with the exact effective key (`record.key`) to inspect or clear a permanent reservation:

```python
reservation = await queue_service.get_task_identity(record.key)
if reservation is not None:
    await queue_service.reset_task_identity(reservation.key)
```

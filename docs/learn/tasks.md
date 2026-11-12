# Defining & Enqueuing Tasks

Tasks are Python functions decorated with `@task` and registered in the process-wide task registry. Both `async def` coroutines and synchronous `def` functions are supported.

## Registering Tasks with the Decorator

Applying `@task` wraps a callable in a [`Task`](../reference/tasks-and-models.md#litestar_queues.task.Task) descriptor while preserving direct invocation (`await generate_invoice("ord-1001", 4999)`) for unit tests and local callers.

```python title="docs/examples/learn/define_and_enqueue.py"
--8<-- "docs/examples/learn/define_and_enqueue.py"
```

### Task Naming & Logical Queues

- **Explicit Name**: Pass a string as the first argument to `@task("billing.generate_invoice", queue="billing")` to assign a stable, refactoring-safe identifier persisted in `QueuedTaskRecord.task_name`.
- **Default Name**: When called as `@task` or `@task()` without a name, the task registers under `function.__name__`.
- **Logical Queue**: The `queue` argument (default `"default"`) determines which logical queue receives the record unless overridden at enqueue time.

## Enqueuing Work via QueueService

[`QueueService`](../reference/plugin-and-config.md#litestar_queues.service.QueueService) is the primary runtime facade injected into Litestar route handlers or opened as an async context manager in scripts and tests.

### Single Enqueue

`await queue_service.enqueue(task_or_name, *args, **kwargs)` persists a [`QueuedTaskRecord`](../reference/tasks-and-models.md#litestar_queues.models.QueuedTaskRecord) in the configured storage backend, dispatches or wakes the target execution transport, and returns a [`TaskResult`](../reference/tasks-and-models.md#litestar_queues.task.TaskResult) handle.

You can pass either the `@task`-decorated object or its registered string name:

- **By Task Object**: `await service.enqueue(generate_invoice, "ord-1001", 4999)` validates and normalizes arguments against the callable signature before persisting.
- **By String Name**: `await service.enqueue("billing.generate_invoice", "ord-1002", 8500, queue="priority-billing")` looks up the task if registered locally, or enqueues by raw name when producer processes do not import worker task modules.

### Bulk Enqueue

When submitting batches of tasks in a single transaction or pipeline round-trip, construct [`TaskRequest`](../reference/tasks-and-models.md#litestar_queues.models.TaskRequest) items and call `await queue_service.enqueue_many(requests)`:

```python
from litestar_queues import TaskRequest

records = await queue_service.enqueue_many(
    [
        TaskRequest(task_name="billing.generate_invoice", args=("ord-2001", 1500), queue="billing"),
        TaskRequest(task_name="billing.generate_invoice", args=("ord-2002", 3200), queue="billing"),
    ]
)
```

`enqueue_many` returns `list[QueuedTaskRecord]` in the exact order of the input sequence.

## Task Module Auto-Discovery

Workers and standalone CLI processes must import the modules where `@task` functions are defined so the task registry is populated before claims arrive. Configure `QueueConfig.task_modules` with module or package dotted paths:

```python
from litestar_queues import QueueConfig

config = QueueConfig(
    queue_backend="memory",
    task_modules=["my_app.tasks", "my_app.billing.jobs"],
)
```

When `QueuePlugin` initializes or `discover_tasks("my_app.tasks")` runs, `litestar-queues` recursively imports packages and modules so every `@task` definition registers deterministically.

## Execution Outcome Semantics

When a worker or one-shot consumer executes a claimed record via `QueueService.execute_record`:

- A return value becomes the completed result persisted on `QueuedTaskRecord.result`, and the task transitions to `completed`.
- A raised exception follows the configured retry policy: retryable exceptions increment `retry_count` and reschedule the record while `retry_count < max_retries`, whereas exhausting retries or calling `non_retryable()` transitions the record to `failed`.

## Querying & Deduplication Key History

Pass `key=` to `QueueService.list_tasks(key=...)` to inspect queued records associated with an explicit or derived deduplication key:

```python
page = await queue_service.list_tasks(key="invoice:ord-1001", limit=20)
```

When a `unique_until="terminal"` deduplication key is reused after an earlier run finishes, historical `key` retention follows each storage backend's native uniqueness implementation:

- **In-Memory, Ephemeral SQLite, Redis, and Valkey**: `InMemoryQueueBackend`, `EphemeralQueueBackend`, `RedisQueueBackend`, and `ValkeyQueueBackend` preserve `key` on historical terminal records, so `list_tasks(key=...)` returns all matching historical and active runs ordered newest-first (`(created_at DESC, id DESC)`).
- **SQLSpec and Advanced Alchemy**: `SQLSpecQueueBackend` and `SQLAlchemyBackend` clear `task_key = None` on superseded terminal rows when a deduplication key is reused, so `list_tasks(key=...)` matches the record currently holding that key.

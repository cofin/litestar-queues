# Task Results & Waiting

Every call to `await queue_service.enqueue(...)` returns a [`TaskResult[T]`](../reference/tasks-and-models.md#litestar_queues.task.TaskResult) handle typed to the task function's return annotation `T`.

## Runnable Example

```python title="docs/examples/learn/results_and_wait.py"
--8<-- "docs/examples/learn/results_and_wait.py"
```

## Inspecting TaskResult Handles

A `TaskResult` wraps the most recently fetched [`QueuedTaskRecord`](../reference/tasks-and-models.md#litestar_queues.models.QueuedTaskRecord) and exposes convenience properties:

| Property / Method | Return Type | Description |
| :--- | :--- | :--- |
| `handle.id` | `UUID` | Unique task identifier (`QueuedTaskRecord.id`) |
| `handle.status` | `TaskStatus` | Cached status (`pending`, `scheduled`, `running`, `completed`, `failed`, `cancelled`, `expired`) |
| `handle.result` | `T \| None` | Cached return value once `status == "completed"` |
| `handle.error` | `str \| None` | Formatted error string from the latest failed attempt |
| `handle.record` | `QueuedTaskRecord \| None` | Full underlying queue record |
| `await handle.refresh()` | `Self` | Reloads `QueuedTaskRecord` from the storage backend in place |
| `await handle.wait(...)` | `T` | Waits for a terminal status and returns `T` (or raises on failure/cancellation/expiration/timeout) |

## Waiting for Completion

`await handle.wait(timeout=10.0, poll_interval=0.05)` blocks until the task reaches a terminal state (`completed`, `failed`, `cancelled`, or `expired`):

1. **Push-Accelerated Waiting**: When the storage backend advertises `capabilities.supports_completion_events == True` (such as `InMemoryQueueBackend`, `RedisQueueBackend`, `ValkeyQueueBackend`, or `SQLSpecQueueBackend` on PostgreSQL with `notify` / `notify_queue`), `wait()` subscribes to completion notifications and wakes immediately when the task finishes.
2. **Polling Fallback**: On backends without completion push notifications, `wait()` polls `backend.get_task(task_id)` every `poll_interval` seconds until the record becomes terminal or `timeout` elapses.

### Exceptions Raised by Wait

| Terminal Outcome | Exception Raised by `await handle.wait()` |
| :--- | :--- |
| `status == "completed"` | None — returns the deserialized result `T` |
| `status == "failed"` | `TaskFailedError` (with `record.error` message) |
| `status == "cancelled"` | `TaskCancelledError` |
| `status == "expired"` | `TaskExpiredError` |
| Record deleted before completion | `TaskNotFoundError` |
| `timeout` elapsed while non-terminal | `TimeoutError` |

## Fetching Records by Task ID

When an HTTP endpoint or CLI command only has a `UUID` (for example from a URL path parameter), look up the record directly through `QueueService.get_task(task_id)`:

```python
record = await queue_service.get_task(task_id)
if record is not None and record.is_terminal:
    print(record.status, record.result)
```

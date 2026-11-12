# Task Results & Waiting

Every call to `await queue_service.enqueue(...)` returns a [`TaskResult[T]`](../reference/tasks-and-models.md#litestar_queues.task.TaskResult) handle typed to the task function's return annotation `T`.

## Runnable Example

```python title="docs/examples/learn/results_and_wait.py"
--8<-- "docs/examples/learn/results_and_wait.py"
```

## Inspecting TaskResult Handles

A `TaskResult` wraps the most recently fetched [`QueuedTaskRecord`](../reference/tasks-and-models.md#litestar_queues.models.QueuedTaskRecord) and exposes convenience properties. Because `TaskResult` caches the snapshot from `enqueue()`, it does not mutate automatically in the background when a remote worker finishes the task. Refresh it after receiving a terminal event (via `await handle.refresh()`) or call `await handle.wait(...)` to fetch the terminal record:

| Property / Method | Return Type | Description |
| :--- | :--- | :--- |
| `handle.id` | `UUID` | Unique task identifier (`QueuedTaskRecord.id`) |
| `handle.status` | `TaskStatus` | Cached status (`pending`, `scheduled`, `running`, `completed`, `failed`, `cancelled`, `expired`) |
| `handle.result` | `T \| None` | Cached return value once `status == "completed"` |
| `handle.error` | `str \| None` | Formatted error string from the latest failed attempt |
| `handle.record` | `QueuedTaskRecord \| None` | Full underlying queue record |
| `await handle.refresh()` | `Self` | Reloads `QueuedTaskRecord` from the storage backend in place |
| `await handle.wait(...)` | `Self` | Waits for a terminal status (`completed`, `failed`, `cancelled`, `expired`), refreshes `handle.record` in place, and returns `self` (or raises `TimeoutError` if `timeout` elapses) |

## Waiting for Completion

`await handle.wait(timeout=10.0, poll_interval=0.1)` blocks until the task reaches a terminal state (`completed`, `failed`, `cancelled`, or `expired`) and returns the refreshed `TaskResult` handle:

1. **Push-Accelerated Waiting**: When the storage backend advertises `capabilities.supports_completion_events == True` (such as `InMemoryQueueBackend`, `RedisQueueBackend`, `ValkeyQueueBackend`, or `SQLSpecQueueBackend` on PostgreSQL with `notify` / `notify_queue`), `wait()` subscribes to completion notifications and wakes immediately when the task finishes.
2. **Polling Fallback**: On backends without completion push notifications, `wait()` polls `backend.get_task(task_id)` every `poll_interval` seconds until the record becomes terminal or `timeout` elapses.

### Outcomes & Exceptions from Wait

| Condition | Behavior of `await handle.wait()` |
| :--- | :--- |
| `status == "completed"` | Returns refreshed `handle` (`handle.result` holds deserialized `T`) |
| `status in {"failed", "cancelled", "expired"}` | Returns refreshed `handle` (`handle.status` and `handle.error` reflect the terminal state) |
| `handle.record is None` before waiting | Raises `RuntimeError("Task record is not available")` |
| `timeout` elapsed while non-terminal | Raises `TimeoutError` |

## Fetching Records by Task ID

When an HTTP endpoint or CLI command only has a `UUID` (for example from a URL path parameter), look up the record directly through `QueueService.get_task(task_id)`:

```python
record = await queue_service.get_task(task_id)
if record is not None and record.is_terminal:
    print(record.status, record.result)
```

## Listing & Filtering Tasks

Inspect operational slices across statuses, logical queues, registered task names, or deduplication keys with `await queue_service.list_tasks(...)`, which returns a [`TaskListPage`](../reference/tasks-and-models.md#litestar_queues.models.TaskListPage):

```python
offset = 0
failed_records = []
while True:
    page = await queue_service.list_tasks(
        status=("failed", "expired"),
        queue="billing",
        task_name="billing.generate_invoice",
        key=None,
        limit=50,
        offset=offset,
    )
    failed_records.extend(page.items)
    if not page.has_more or not page.items:
        break
    offset += len(page.items)
```

- **Page Contract**: `TaskListPage` exposes `items` (`list[QueuedTaskRecord]`), `limit`, `offset`, `total` (count of all matching records across pages), and the `has_more` property (`(offset + len(items)) < total`).
- **Deterministic Ordering**: Results are ordered newest-first by `(created_at DESC, id DESC)` across every storage backend.
- **Validation & Empty Status Fast Path**: `limit` defaults to `50` (`>= 1`) and `offset` defaults to `0` (`>= 0`); passing booleans, out-of-range integers, or unknown `status` strings raises `QueueConfigurationError`. Passing an empty status sequence (`status=()`) returns `TaskListPage(items=[], limit=limit, offset=offset, total=0)` without hitting storage.

From the command line, inspect the same filtered slices against persistent backends using `litestar queues list`:

```bash
litestar --app app:app queues list --status failed --queue billing --limit 25 --json
```

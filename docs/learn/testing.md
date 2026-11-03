# Testing Tasks & Workers

`litestar-queues` is designed for fast, deterministic unit and integration testing without requiring external brokers or background daemon threads in test suites.

## Runnable Example

```python title="docs/examples/learn/testing_tasks.py"
--8<-- "docs/examples/learn/testing_tasks.py"
```

## Testing Layers

### Direct Callable Invocation

Because `@task` preserves the underlying function signature and `await task_fn(*args, **kwargs)` delegates directly to the wrapped callable, pure domain logic can be unit tested without initializing `QueueService`:

```python
result = await calculate_tax(10_000, 825)
assert result == 825
```

### Inline Execution with Immediate Mode

To test the full enqueue-to-completion pipeline (argument serialization, uniqueness checks, `task_dependency_provider` lifecycle, event publication, and `TaskResult` updates) synchronously inside an HTTP test request, combine `queue_backend="memory"`, `execution_backend="immediate"`, and `WorkerConfig(placement="external")`:

```python
from litestar_queues import QueueConfig, WorkerConfig

test_config = QueueConfig(
    queue_backend="memory",
    execution_backend="immediate",
    worker=WorkerConfig(placement="external"),
)
```

With `execution_backend="immediate"`, every `await queue_service.enqueue(...)` persists the record, claims it, runs `execute_record()` inline before `enqueue()` returns, and transitions the record to `completed` (or `failed` / `cancelled`). Setting `WorkerConfig(placement="external")` prevents `AsyncTestClient` from spawning a background polling worker during the test.

### Deterministic Single-Claim Testing with consume_one

When testing queued state transitions, retries, expiration deadlines, or custom worker hooks where the record must first sit in `pending` or `scheduled` status, use `execution_backend="local"` with [`consume_one`](../reference/execution-and-workers.md#litestar_queues.consumer.consume_one):

```python
from litestar_queues import TaskExitCode, consume_one

handle = await service.enqueue(calculate_tax, 50_000, 825)
assert handle.status == "pending"

exit_code = await consume_one(service, handle.id)
assert exit_code == TaskExitCode.SUCCESS

await handle.refresh()
assert handle.status == "completed"
```

`consume_one(service, task_id)` claims the specific record by `UUID`, executes a single attempt with heartbeat management, and returns a [`TaskExitCode`](../reference/execution-and-workers.md#litestar_queues.consumer.TaskExitCode) (`SUCCESS`, `FAILURE`, `MISSING_RECORD`, `UNKNOWN_TASK`, `CLAIM_LOST`, or `CANCELLED`).

## Isolating the Task Registry in Pytest Suites

If tests dynamically register temporary `@task` definitions, snapshot and restore the task registry or call `clear_task_registry()` in a fixture so temporary task names do not leak across test modules. For event assertions with `InMemoryQueueEventSink` and `EventCapture`, see [Testing Queue Events](../events/testing.md).

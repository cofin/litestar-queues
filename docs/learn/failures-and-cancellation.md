# Failures, Retries & Cancellation

`litestar-queues` provides deterministic retry policies, explicit terminal exit helpers, and cooperative + preemptive cancellation across both local workers and remote serverless execution providers.

--8<-- "tools/docs/includes/diagram-task-lifecycle.html"

## Runnable Example

```python title="docs/examples/learn/failures_and_cancellation.py"
--8<-- "docs/examples/learn/failures_and_cancellation.py"
```

## Retries & Exponential Backoff

Configure `retries` (maximum retry attempts after the initial attempt fails) and `retry_backoff` on `@task`:

- **Constant Delay**: Pass a `float` in seconds (`retry_backoff=5.0`) to wait a fixed duration before every retry.
- **Exponential Backoff**: Pass [`RetryBackoff(initial_delay=1.0, multiplier=2.0, max_delay=30.0)`](../reference/tasks-and-models.md#litestar_queues.task.RetryBackoff). For zero-based `retry_count`, the delay in seconds is computed as:

$$\text{delay} = \min\left(\text{max\_delay},\ \text{initial\_delay} \times \text{multiplier}^{\text{retry\_count}}\right)$$

When a retryable exception is raised and `record.retry_count < record.max_retries`:

1. `retry_count` increments by `1` (which also advances the lease fence counter).
2. If the computed backoff delay is greater than `0`, the record transitions to `scheduled` with `scheduled_at = now + delay` and emits `task.retried`.
3. If the delay is `0`, the record transitions directly to `pending` for immediate reclaiming.

## Explicit Terminal Exits Inside a Task

Task bodies can bypass the remaining retry budget when a failure is permanent or an operation is intentionally aborted:

- **`non_retryable(message)`**: Raises `NonRetryableError`. `QueueService.execute_record` catches it, skips all remaining retries, transitions the record immediately to `failed`, and emits `task.failed` with `will_retry=False`.
- **`job_cancelled(message)`**: Raises `JobCancelledError`. `QueueService.execute_record` transitions the running record directly to `cancelled` and emits `task.cancelled`.

## Cancelling Tasks via QueueService

Call `await queue_service.cancel_task(task_id, include_running=False)` to cancel a task from an HTTP route handler, CLI command, or maintenance job:

- **Queued Tasks (`include_running=False`, default)**: Atomically transitions `pending` or `scheduled` records to `cancelled`. Returns `True` only for the caller that wins the state transition, or `False` if the task is already `running` or terminal.
- **Running Tasks (`include_running=True`)**: Also permits `running` records to transition to `cancelled`, signals the owning worker (or remote execution provider), and cancels the in-flight execution coroutine.

### Worker-Control Wakeup Transport Matrix

When `cancel_task(task_id, include_running=True)` marks a running record `cancelled`, the storage backend notifies active workers to abort the in-flight task immediately rather than waiting for the next `WorkerConfig.cancellation_poll_interval` (default `1.0s`):

| Backend Family | Worker-Control Transport | Delivery Mechanism |
| :--- | :--- | :--- |
| **Redis, Valkey** | Redis Pub/Sub control channel | Instant push wakeup on `<prefix>:control` with fallback poll |
| **SQLSpec on PostgreSQL** | PostgreSQL `LISTEN` / `NOTIFY` | Push notification on the control channel when `notify` or `notify_queue` is active |
| **memory** | In-process `asyncio.Event` | Immediate in-process wakeup across subscribed local workers |
| **everything else** | Periodic durability poll | Workers reconcile active claims every `WorkerConfig.cancellation_poll_interval` (`1.0s`) |

### Remote Provider Cancellation Outcomes

When a task runs on an external execution backend (`cloudtasks` or `cloudrun`) and holds an active provider reference (`QueuedTaskRecord.execution_ref`), `QueueService.cancel_task` asks the execution backend to cancel the remote resource first and classifies the response via `ExecutionCancelResult`:

| Provider Outcome | Permits Durable `cancelled` Transition | Runtime Behavior |
| :--- | :--- | :--- |
| `accepted` | Yes | Remote Cloud Task or Cloud Run execution was deleted/cancelled; storage record transitions to `cancelled` |
| `already_cancelled` | Yes | Remote resource was already absent or terminated; storage record transitions to `cancelled` |
| `unsupported` | Yes | Execution backend is local or has no remote resource to cancel; storage record transitions to `cancelled` |
| `retryable` | No (`cancel_task` returns `False`) | Transient provider API failure; emits `task.event` warning (`<backend>.cancel_failed`) and preserves current record state |

## Cooperative Cancellation Checkpoints

Long-running tasks that perform multi-step I/O or batch processing can inspect [`TaskExecutionContext`](../reference/events.md#litestar_queues.events.TaskExecutionContext) via `get_current_task_context()` (or `current_task_context()`):

- **`context.is_cancelled`**: Boolean property reflecting whether the current attempt has been cancelled or lost its heartbeat claim.
- **`context.raise_if_cancelled()`**: Synchronously raises `JobCancelledError` (or `asyncio.CancelledError` on claim loss) at a safe checkpoint.
- **`await context.wait_cancelled()`**: Coroutine that suspends until cancellation is signalled, useful inside `asyncio.wait(..., return_when=FIRST_COMPLETED)` races.

## Sanitizing Persisted Error Messages

To strip sensitive tokens or format tracebacks before exceptions are persisted to `QueuedTaskRecord.error` and emitted in `task.failed` events, configure `QueueConfig.error_sanitizer`:

```python
from litestar_queues import QueueConfig
from litestar_queues.models import QueuedTaskRecord


def sanitize_task_error(exc: BaseException, record: QueuedTaskRecord) -> str:
    return f"{record.task_name} failed with {type(exc).__name__}"


config = QueueConfig(queue_backend="memory", error_sanitizer=sanitize_task_error)
```

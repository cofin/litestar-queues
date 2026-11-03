# Workers & Execution Lifecycle

Workers claim due `pending` and `scheduled` records from the storage backend, manage heartbeat leases, execute task callables within bounded concurrency pools, and run background sweeps.

--8<-- "tools/docs/includes/diagram-worker-placements.html"

## Runnable Example

```python title="docs/examples/learn/worker_placements.py"
--8<-- "docs/examples/learn/worker_placements.py"
```

## Choosing a Worker Placement

[`WorkerConfig.placement`](../reference/plugin-and-config.md#litestar_queues.config.WorkerConfig) controls which process owns the worker loop:

| Placement | Owner Process | Scaling Multiplier | Recommended Use Case |
| :--- | :--- | :--- | :--- |
| `"server"` (default) | Litestar CLI server lifespan (`litestar run`) | `1` supervised worker subprocess per `litestar run` invocation | Local development and single-node deployments using `litestar run --workers N` |
| `"asgi"` | Application ASGI lifespan | `1` in-process worker per ASGI web worker (`N` web workers = `N` queue workers) | Direct `uvicorn`/`granian` multi-worker deployments or `InMemoryQueueBackend` single-process apps |
| `"external"` | None started inside the web server | Scaled independently via `litestar queues run` or serverless transports | Production deployments separating web traffic from worker pods, or scale-to-zero Cloud Tasks / Cloud Run Jobs |

## Standalone CLI Entrypoint & App Lifespan Parity

When running dedicated worker, consumer, or maintenance processes via `litestar --app my_app:app queues run` (implemented in `litestar_queues.cli`), `WorkerConfig.run_app_lifespan` (default `True`) enters `QueuePlugin.standalone_app_lifespan` before opening the worker.

Entering the application lifespan ensures that shared resources initialized in Litestar `lifespan` context managers or `on_startup` hooks (such as database pools, HTTP clients, and telemetry exporters) are available to tasks in standalone `external` workers just as they are in `asgi` placement. Set `run_app_lifespan=False` only when tasks manage all external connections through `task_dependency_provider`.

## Concurrency & Per-Queue Caps

- **Global Concurrency (`max_concurrency: int = 1`)**: Maximum number of tasks executing concurrently inside one worker instance.
- **Per-Queue Caps (`queue_concurrency: Mapping[str, int]`)**: Optional dictionary capping how many concurrent slots a specific logical queue may consume on that worker. Each entry must satisfy `1 <= cap <= max_concurrency`.
- **Claim Batch Size (`batch_size: int = 10`)**: Maximum records claimed in a single storage round-trip, automatically bounded by available global and per-queue concurrency slots.

## Adaptive Polling & Push Wakeups

Workers combine push wakeup notifications (when `backend.capabilities.supports_worker_wakeups` is `True`) with adaptive exponential polling backoff:

- **`poll_interval: float = 0.1`**: Base delay in seconds after an idle poll when work was recently seen.
- **`poll_backoff_max: float | None = 30.0`**: Upper bound in seconds for exponential backoff during sustained idle periods (`None` disables backoff and keeps a fixed `poll_interval`).
- **`poll_backoff_multiplier: float = 2.0`**: Factor by which the poll sleep increases after each consecutive empty claim pass.
- **`poll_jitter: float = 0.15`**: Symmetric random jitter ratio (`0.0` to `1.0`) applied to poll sleep durations to prevent thundering-herd queries across worker fleets.

Any push wakeup notification or non-empty claim immediately resets the poll interval back to `poll_interval`.

## Heartbeats, Lease Fencing & Stale Recovery

Heartbeat timestamps are automatic: while a task is `running`, `WorkerHeartbeatManager` periodically extends its lease in bulk:

1. **Bulk Heartbeat Writes**: Every `heartbeat_interval` seconds (default `30.0`, with `heartbeat_jitter_fraction=0.1`), the worker sends a batch of [`HeartbeatTouch`](../reference/tasks-and-models.md#litestar_queues.models.HeartbeatTouch) updates fenced on `expected_retry_count`.
2. **Claim-Loss Protection**: If a task's status changed externally or another worker reclaimed the record after a network partition, the storage backend reports the task in `HeartbeatTouchResult.missed_task_ids`. After `heartbeat_miss_threshold` (default `2`) consecutive misses (or an immediate fence mismatch), the worker emits `task.claim_lost` and, when `cancel_on_claim_loss=True` (default), cancels the local coroutine to close the duplicate-execution window.
3. **Stale Recovery Sweep**: When `stale_after` is configured (for example `stale_after=60.0`), the worker sweeps for `running` records whose `heartbeat_at` is older than `stale_after` every `stale_check_interval` seconds (`60.0`).
   - Tasks with remaining retry budget (`retry_count < max_retries`) and `requeue_on_stale=True` increment `retry_count`, apply `QueueConfig.stale_requeue_priority` (`"preserve"`, an `int` ceiling clamp, or a callable), and return to `pending`/`scheduled`.
   - Tasks that have exhausted retries transition to `failed` as a stale failure, emit `task.stale_failed`, and invoke `@task(..., on_stale_failure=...)` if registered.

## In-Worker Periodic Terminal Cleanup

Workers can automatically purge old terminal records (`completed`, `failed`, `cancelled`, `expired`) without requiring an external maintenance cron job:

- **`WorkerConfig.terminal_retention: float | None = None`**: Age threshold in seconds after `completed_at` before a terminal record is eligible for deletion. When left as `None`, the worker automatically inherits `backend_config.terminal_ttl` from `SQLSpecBackendConfig`, `RedisBackendConfig`, or `ValkeyBackendConfig` when set.
- **`WorkerConfig.terminal_check_interval: float = 300.0`**: Interval in seconds between in-worker terminal record cleanup sweeps.

Permanent `unique_until="forever"` reservations (`TaskReservation`) are never deleted by terminal cleanup sweeps.

## Graceful Shutdown & Interruption Budgets

When a worker receives `SIGINT` or `SIGTERM`:

1. **Graceful Drain (`graceful_shutdown_timeout: float = 30.0`)**: The worker stops claiming new tasks and waits up to `graceful_shutdown_timeout` seconds for active coroutines to finish naturally.
2. **Task Cancellation (`final_cancel_timeout: float = 5.0`)**: Any tasks still running after the graceful drain window are cancelled and given `final_cancel_timeout` seconds to unwind `finally:` blocks and dependency providers.
3. **Shutdown Requeue (`requeue_on_shutdown: bool = False`)**: When enabled, interrupted tasks are returned to `pending` via `QueueService.interrupt_task` without consuming their failure retry budget, up to `max_interruptions` (default `3`). Effective failure attempts are tracked separately from shutdown interruptions (`attempts_consumed = retry_count - interruptions`) until `max_interruptions` is reached, at which point further interruptions route through the normal retry policy.
4. **Hard Watchdog (`hard_exit_timeout: float | None = 10.0`)**: Enforces a strict wall-clock exit deadline on the supervised worker subprocess if native C extensions or stuck threads refuse to terminate.

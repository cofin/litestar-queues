# Queue Events Overview

`litestar-queues` decouples task execution state (`QueuedTaskRecord`) from realtime event publishing ([`QueueEventsConfig`](../reference/events.md#litestar_queues.events.QueueEventsConfig)). Tasks and workers emit typed [`QueueEvent`](../reference/events.md#litestar_queues.events.QueueEvent) envelopes that can simultaneously fan out over Litestar `ChannelsPlugin` SSE/WebSocket streams and persist into backend-owned durable event history (`QueueEventLog`).

--8<-- "tools/docs/includes/diagram-events-and-streams.html"

## Runnable Example

```python title="docs/examples/events/publish_from_task.py"
--8<-- "docs/examples/events/publish_from_task.py"
```

## Five Distinct Execution Signals

| Signal | Emitter API | Wire Event Type | Purpose |
| :--- | :--- | :--- | :--- |
| **Heartbeat** | Automatic `WorkerHeartbeatManager` + optional `ctx.beat(detail)` | Storage `heartbeat_at` + metadata | Extends the running task's lease fence in storage; `ctx.beat(detail)` records last-value-wins diagnostic text on the next heartbeat write and does not update progress or terminal task state |
| **Progress** | `await ctx.progress(...)` / `await publish_task_progress(...)` | `"task.progress"` | Emits structured progress (`current`, `total`, `percent`, `message`, `payload`) to live subscribers and durable history |
| **Log** | `await ctx.log(...)` / `await publish_task_log(...)` | `"task.log"` | Emits task-scoped structured log entries (`level`, `message`, `payload`) |
| **Custom Event** | `await ctx.event(name, ...)` / `await publish_task_event(name, ...)` | Caller-defined (e.g. `"crawl.page_discovered"`) | Emits domain-specific milestone events during task execution |
| **Lifecycle Event** | Worker / `QueueService` runtime | `"task.started"`, `"task.completed"`, `"task.failed"`, `"task.retried"`, `"task.cancelled"`, `"task.expired"`, `"task.interrupted"`, `"task.claim_lost"`, `"task.stale_failed"` | Authoritative state-transition events emitted automatically by `QueueService` after storage commits |

## Publishing from Inside a Task

### Automatic Context Injection

Declare a parameter named `_task_context: TaskExecutionContext` on any `@task` function to receive the attempt's bound [`TaskExecutionContext`](../reference/events.md#litestar_queues.events.TaskExecutionContext):

```python
from litestar_queues import TaskExecutionContext, task


@task("crawler.crawl_domain", queue="crawler")
async def crawl_domain(domain: str, _task_context: TaskExecutionContext) -> dict[str, str]:
    ctx = _task_context
    ctx.beat("fetching-sitemap")
    await ctx.progress(
        current=3,
        total=6,
        message="3/6 pages",
        payload={"page": 3},
    )
    await ctx.event(
        "crawl.page_discovered",
        payload={"url": f"https://{domain}/docs"},
        immediate=False,
    )
    return {"domain": domain, "status": "complete"}
```

### Module-Level ContextVar Helpers

When helper functions called deep inside a task do not pass `_task_context` down the call stack, use the module-level helpers from `litestar_queues.events`:

- `await publish_task_progress(current=..., total=..., message=..., payload=..., strict=False)`
- `await publish_task_log(message, level="info", payload=..., strict=False)`
- `await publish_task_event(event_type, message=..., payload=..., immediate=False, strict=False)`
- `beat(detail)`

By default (`strict=False`), calling these helpers outside an active task execution (for example when unit testing a helper function directly) safely no-ops and returns `None`. Pass `strict=True` (or call `require_current_task_context()`) to raise `RuntimeError` if no `TaskExecutionContext` is bound.

## Live Delivery vs Durable Event History

[`QueueEventsConfig`](../reference/events.md#litestar_queues.events.QueueEventsConfig) composes three independent capabilities:

1. **Live Delivery (`delivery=EventDeliveryConfig(...)`)**: Buffers events in an in-memory ring buffer (`EventBufferConfig`) and flushes them to Litestar `ChannelsPlugin` (`ChannelsQueueEventSink`) and any custom `sinks`. Setting `EventDeliveryConfig(strict=True)` propagates sink delivery exceptions to the caller instead of logging and isolating them.
2. **Browser Stream Routes (`stream=EventStreamConfig(...)`)**: Mounts ready-to-use SSE and WebSocket endpoints under `/queues/events` (see [Realtime SSE & WebSocket Streams](streams.md)).
3. **Durable Event History (`history=EventHistoryConfig(...)`)**: Persists events into the storage backend's `QueueEventLog` (`queue_task_event_history` in SQLSpec/Advanced Alchemy or Redis/Valkey Streams + Hashes) and enforces a **commit-before-live** barrier so browser subscribers never receive a lifecycle event before its history record is durably committed (see [Durable Event History](history.md)).

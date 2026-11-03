# Standalone Event Publishing

The `litestar_queues.events` subpackage can be used outside a queue worker—for example in CLI scripts, webhook receivers, or external job runners—using `bind_task_context`, `bind_beat_sink`, and `create_event_producer`.

## Runnable Example

```python title="docs/examples/events/standalone_events.py"
--8<-- "docs/examples/events/standalone_events.py"
```

## Binding TaskExecutionContext & TaskBeatSink

When running domain functions that call `publish_task_progress`, `publish_task_log`, `publish_task_event`, or `beat()` inside a custom harness without a `Worker`:

1. **`bind_task_context(context)`**: Context manager that binds a [`TaskExecutionContext`](../reference/events.md#litestar_queues.events.TaskExecutionContext) to the current asyncio `ContextVar` scope so `get_current_task_context()`, `require_current_task_context()`, and the `publish_task_*` helpers resolve to `context`.
2. **`bind_beat_sink(sink)`**: Context manager that binds a [`TaskBeatSink`](../reference/events.md#litestar_queues.events.TaskBeatSink) (`record_beat(task_id: str, detail: str | None) -> None`) to receive synchronous `context.beat(detail)` or `beat(detail)` calls.

## Three-Tier Actor Attribution Precedence

Every `QueueEvent` can carry a [`QueueEventActor(type="user" | "system" | "service", id="...")`](../reference/events.md#litestar_queues.events.QueueEventActor) identifying who initiated or emitted the event. Actor attribution resolves in strict three-tier precedence order:

| Precedence | Source | Scope | Example |
| :--- | :--- | :--- | :--- |
| Highest | Per-call `actor=` argument | Single emitted event | `await ctx.progress(current=1, total=2, actor=QueueEventActor(type="user", id="u-42"))` |
| Second | `TaskExecutionContext.actor` | Current execution attempt | `ctx.actor = QueueEventActor(type="service", id="billing-worker")` |
| Lowest | `@task(..., actor=...)` | Task definition default (static `QueueEventActor` or zero-arg callable) | `@task("sync", actor=QueueEventActor(type="system", id="nightly-cron"))` |

## External Event Producer (`create_event_producer`)

To publish scoped events from a CLI command or auxiliary microservice without opening a full `QueueService`, use `async with create_event_producer(config) as producer:`:

- **`producer.task(task_id)`**: Returns a task-scoped handle supporting `.progress(...)`, `.log(...)`, `.event(...)`, and `.publish(...)`.
- **`producer.queue(queue_name)`**: Publishes events scoped to a logical queue (`scope="queue"`).
- **`producer.worker(worker_id)`**: Publishes events scoped to a worker (`scope="worker"`).
- **`producer.channel(scope_key)`**: Publishes events to a custom channel key (`scope="custom"`, `scope_key=scope_key`).

`create_event_producer(config)` opens any configured `QueueEventsConfig.channels` and custom `EventDeliveryConfig.sinks` on `__aenter__`, starts the event delivery buffer, and drains and closes acquired resources in reverse order on `__aexit__`.

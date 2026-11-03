# Testing Queue Events

Testing event emission and realtime stream routes requires deterministic, synchronous delivery without background buffer timers or flakiness.

## Runnable Example

```python title="docs/examples/events/testing_events.py"
--8<-- "docs/examples/events/testing_events.py"
```

## Deterministic Unit Testing Recipe

Combine four settings in unit tests to capture every task and lifecycle event synchronously in memory:

1. **`InMemoryQueueEventSink()`**: Appends every delivered [`QueueEvent`](../reference/events.md#litestar_queues.events.QueueEvent) to `sink.events` (`list[QueueEvent]`).
2. **`EventDeliveryConfig(sinks=(sink,), buffer=None, strict=True)`**: Disables background ring-buffer batching (`buffer=None`) so every `await ctx.progress(...)` or lifecycle transition delivers to `sink.events` before the `await` returns, and sets `strict=True` so any sink assertion error propagates immediately.
3. **`execution_backend="immediate"`**: Runs the task body inline during `await service.enqueue(...)`.
4. **`WorkerConfig(placement="external")`**: Prevents background polling loops from starting during the test.

## Testing Durable Event History

When testing `QueueEventLog.query_events()` or `summarize_stages()` with `EventHistoryConfig(...)`, call `await event_log.flush_events()` before querying non-terminal progress or log events, or await terminal task completion (which flushes the history buffer automatically via the commit-before-live barrier):

```python
event_log = service.get_event_log()
assert event_log is not None
await event_log.flush_events()
page = await event_log.query_events(QueueEventQuery(task_id=str(handle.id)))
```

## Testing SSE & WebSocket Stream Endpoints

Use Litestar's `AsyncTestClient` to test `EventStreamConfig` routes and `channel_authorizer` callbacks end-to-end:

- **WebSocket Streams**: Connect with `with client.websocket_connect("/queues/events/tasks/<task_id>") as ws:` and assert on `ws.receive_json()`.
- **SSE Streams**: Pass `max_stream_duration=0.2` on `EventStreamConfig` in tests so `await client.get("/queues/events/sse/tasks/<task_id>")` closes cleanly after emitting initial frames without hanging the test coroutine.

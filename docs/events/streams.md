# Realtime SSE & WebSocket Streams

Configuring [`EventStreamConfig`](../reference/events.md#litestar_queues.events.EventStreamConfig) on `QueueEventsConfig.stream` mounts ready-to-use Server-Sent Events (SSE) and WebSocket endpoints on your Litestar application, powered by Litestar's `ChannelsPlugin`.

## Runnable Configuration Example

```python title="docs/examples/events/stream_routes_config.py"
--8<-- "docs/examples/events/stream_routes_config.py"
```

## Mounted Stream Endpoints

By default (`path="/queues/events"`, or `/{namespace}/events` when a custom `QueueNamespace` is active), `QueuePlugin` registers endpoints for each enabled scope (`scopes={"task", "queue", "worker", "global", "custom"}`) and transport (`transports={"sse", "websocket"}`):

| Scope | WebSocket Path | SSE Path | Channel Topic Subscribed |
| :--- | :--- | :--- | :--- |
| `"global"` | `WS /queues/events` | `GET /queues/events/sse` | Global queue event channel |
| `"task"` | `WS /queues/events/tasks/{task_id:str}` | `GET /queues/events/sse/tasks/{task_id:str}` | Canonical task channel for `task_id` |
| `"queue"` | `WS /queues/events/queues/{queue_name:str}` | `GET /queues/events/sse/queues/{queue_name:str}` | Canonical logical queue channel |
| `"worker"` | `WS /queues/events/workers/{worker_id:str}` | `GET /queues/events/sse/workers/{worker_id:str}` | Canonical worker lifecycle channel |
| `"custom"` | `WS /queues/events/channels/{scope_key:str}` | `GET /queues/events/sse/channels/{scope_key:str}` | Adopter-defined custom scope key channel |

## Stream Authorization & Guards

Protect stream endpoints against unauthorized subscriptions using `guards` and `channel_authorizer` on `EventStreamConfig`:

- **Route Guards (`guards=[...]`)**: Standard Litestar `Guard` callables evaluated before the SSE or WebSocket handshake proceeds.
- **Per-Channel Authorizer (`channel_authorizer`)**: Async or sync callable `(connection: ASGIConnection, scope: QueueEventScope, key: str | None) -> bool` invoked with the requested scope and path key (`task_id`, `queue_name`, `worker_id`, or `scope_key`). Returning `False` rejects the subscription with `403 Forbidden` (or closes the WebSocket).
- **Unauthenticated Policy (`unauthenticated_access`)**: Defaults to `"warn"` (emits a startup warning if neither `guards` nor `channel_authorizer` is configured). Set to `"error"` to fail startup if streams are unprotected, or `"allow"` in local demos.

## Planned Stream Rollover, Keepalives & Shielded Cleanup

Long-lived SSE and WebSocket connections traverse reverse proxies and serverless platforms (such as Google Cloud Run's 60-minute request ceiling) that forcibly reset idle or expired connections:

- **Planned Rollover (`max_stream_duration: float | None = None`)**: When set (for example `max_stream_duration=3000.0` for a 50-minute ceiling under a 60-minute proxy timeout), the stream endpoint cleanly closes the connection after `max_stream_duration` seconds so browser `EventSource` or WebSocket clients reconnect smoothly before an abrupt proxy `504` reset.
- **SSE Reconnect Hint (`retry_ms: int | None = None`)**: When set (for example `retry_ms=3000`), SSE responses emit an initial `retry: 3000` frame instructing the browser `EventSource` to wait 3 seconds before reconnecting, and echo each event's `event_id` in the SSE `id:` field so reconnecting clients send `Last-Event-ID`.
- **Keepalive Pings (`heartbeat_interval: float = 25.0`)**: Emits periodic `"ping"` frames every 25 seconds so intermediate load balancers do not drop quiet subscriptions.
- **Shielded Disconnect Teardown**: On client disconnect or cancellation, stream handlers shield `ChannelsPlugin` unsubscription (`anyio.move_on_after(5.0, shield=True)`) so subscriber queues never leak under abrupt client disconnects.

## Wire JSON Envelope (camelCase)

Browser stream frames serialize `QueueEvent` fields in camelCase JSON for direct consumption in TypeScript, HTMX, and browser frontends:

```json
{
  "id": "8f2a9c4e1b0d4f3a9e8c7b6a5d4f3e21",
  "eventKey": "8f2a9c4e1b0d4f3a9e8c7b6a5d4f3e21",
  "type": "task.progress",
  "scope": "task",
  "scopeKey": "tenant:acme",
  "taskId": "2c8d8f72-6a4b-4c5c-9c82-5e3f21a09b41",
  "taskName": "crawler.index_site",
  "queue": "crawler",
  "workerId": "worker-1",
  "executionBackend": "local",
  "executionProfile": null,
  "attempt": 1,
  "sequence": 2,
  "level": null,
  "message": "3/6 pages",
  "progressCurrent": 3.0,
  "progressTotal": 6.0,
  "progressPercent": 50.0,
  "occurredAt": "2026-11-03T19:30:00+00:00",
  "payload": {
    "page": 3
  }
}
```

## Multi-Process Channels Topology

- **Single-Process (`placement="asgi"` with `MemoryChannelsBackend`)**: In single-process development or demos, an in-memory `ChannelsPlugin(backend=MemoryChannelsBackend())` delivers worker events directly to browser streams in the same process.
- **Multi-Process / External Workers (`placement="server"` or `"external"`)**: When workers run in separate OS processes or containers from web workers, configure `ChannelsPlugin` with a shared cross-process backend (such as `RedisChannelsStreamBackend` or `RedisChannelsPubSubBackend`) on both the web application and worker configuration so events published by workers reach browser clients connected to any web worker.

## Complete Runnable HTMX + Vite Examples

The repository includes 10 complete runnable HTMX + `litestar-vite` realtime streaming applications under `examples/`:

- **In-Memory Single-Process Variants**:
  - WebSocket app: `examples/htmx_realtime_websocket/app.py` (frontend entry `examples/shared/resources/main-ws.ts` and partial `examples/shared/templates/partials/stream_mount_ws.html`)
  - SSE app: `examples/htmx_realtime_sse/app.py` (frontend entry `examples/shared/resources/main-sse.ts` and partial `examples/shared/templates/partials/stream_mount_sse.html`)
  - Shared dashboard template: `examples/shared/templates/index.html`
- **Persistent Backend Variants**:
  - `examples/htmx_realtime_websocket_sqlspec` and `examples/htmx_realtime_sse_sqlspec`
  - `examples/htmx_realtime_websocket_advanced_alchemy` and `examples/htmx_realtime_sse_advanced_alchemy`
  - `examples/htmx_realtime_websocket_redis` and `examples/htmx_realtime_sse_redis`
  - `examples/htmx_realtime_websocket_valkey` and `examples/htmx_realtime_sse_valkey`

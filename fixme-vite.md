# FIXME: adopt `litestar-vite-plugin` stream helpers

Follow-on work, blocked on a `litestar-vite-plugin` release (needs >= 0.28.0). This repo currently pins `^0.26.1` in the example `package.json` files.

## What's coming

`litestar-vite-plugin/helpers` will export a transport-agnostic stream primitive, plus a `<litestar-stream>` custom element and React/Vue/Svelte adapters:

```ts
import { createEventStream } from "litestar-vite-plugin/helpers"

const stream = createEventStream({
  buildUrl: () => `/queues/events/sse/tasks/${taskId}`,
  transport: "sse",
  onEvent: (frame) => render(frame),
  onGap: ({ missing }) => warn(`Missed ${missing} updates`),
})
stream.connect()
```

It handles what every example here hand-writes today: reconnect with jittered backoff, health transitions, `{"type":"ping"}` filtering, duplicate suppression, and sequence-gap warning.

Defaults are built against this package's wire format — `eventKey ?? id` for dedup, `type === "ping"` for heartbeats, `sequence` chained per `(taskId, attempt)`.

## The cleanup

`examples/htmx_realtime_*/resources/main.ts` is duplicated across 10 example apps — 2 distinct files, 5 byte-identical copies each:

```
5 × htmx_realtime_sse*/resources/main.ts        md5 20b05280eb95746c5a17d2aba6e2a571
5 × htmx_realtime_websocket*/resources/main.ts  md5 2c72e0c71255856a28b7c77017d32aee
```

The two differ only in transport wiring (`htmx-ext-sse` + `sse-connect` vs `htmx-ext-ws` + `ws-connect`, `sseBeforeMessage` vs `wsBeforeMessage`). The JSON handling — `parseQueueEvent`, ping filter, `handleQueueEvent` — is character-for-character identical.

All of it collapses to one import. The `<litestar-stream>` element also removes the htmx-2 global-attach dance the examples currently document in a 9-line comment, because `connectedCallback`/`disconnectedCallback` gives the same "connection lifecycle == swap lifecycle" property natively — see the comment in `templates/partials/stream_mount.html`.

## Upstream asks (this repo's side)

Three things here would make the client helper meaningfully better. None block it.

### 1. SSE frames carry no `id:`

`_sse_frame` (`src/litestar_queues/events/streaming.py:436-454`) emits `event:` and `data:` only. Without an `id:` field, `Last-Event-ID` resume cannot work, so a reconnecting browser has no way to say where it left off — recovery depends entirely on `replay_limit` being generous enough.

Emitting `id: <event.id>` would let native EventSource resume do the work.

### 2. SSE event names must be enumerated client-side

Because `_sse_frame` sets `event: <event.type>`, and `EventSource.onmessage` fires **only for unnamed events**, a client must call `addEventListener` once per event name it wants. That is why the examples hardcode:

```html
sse-swap="task.started,task.progress,task.log,crawl.page_discovered,task.completed,task.failed"
```

Custom types from `ctx.event("crawl.page_discovered")` are unknowable ahead of time, so no client-side default can be complete. The helper will ship the 11 known `QueueEventType` values as a default list and let callers extend it, but that is a workaround.

Options worth considering: emit an *additional* unnamed frame, or put the type solely in the JSON body and use a single fixed SSE event name. Either makes "subscribe to everything" possible.

### 3. `sequence` is only set on task-context events

`sequence` is assigned exclusively in `TaskExecutionContext.publish()` (`src/litestar_queues/events/context.py:142`). There are zero `sequence=` assignments in `events/producer.py`, `service.py`, or `execution/cloudrun/backend.py`, so worker- and service-emitted lifecycle events (`task.started`, `task.completed`, `task.failed`, `task.cancelled`, `worker.heartbeat`) go out with `sequence: null`, interleaved among numbered frames.

The client gap detector works around this by ignoring null-sequence frames rather than treating them as chain breaks. That is correct but fragile — it depends on an undocumented invariant. Worth either documenting it as a guarantee with a test, or assigning sequences on all task-scoped events.

## Also worth documenting here

Two behaviors that are load-bearing for any client and currently undocumented:

- **Server dedup is per-connection.** The dedup set is built inside `_pump_events` (`events/streaming.py:160`), fresh for each connection, so replayed frames after a reconnect *are* re-sent. Clients must dedup with a window that outlives the socket, or they duplicate up to `replay_limit` events into the UI on every reconnect.
- **Replay is off by default.** `replay_limit` defaults to `0` (`events/stream_config.py:58`) and the Channels backend needs its own `history=` (the examples use `MemoryChannelsBackend(history=200)`). Silent recovery across a reconnect only happens when both are configured.

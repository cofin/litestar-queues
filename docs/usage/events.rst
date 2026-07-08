======
Events
======

Queue events deliver task lifecycle, progress, log, and custom updates to
application-owned realtime infrastructure. They are distinct from queue backend
wakeup notifications.

Enable Events
=============

Events are disabled when ``QueueConfig.event`` is ``None``. Providing
``EventConfig`` enables them by default unless ``enabled=False`` is explicit:

.. code-block:: python

   from litestar_queues import QueueConfig
   from litestar_queues.events import EventConfig


   config = QueueConfig(
       event=EventConfig(
           channels_backend=channels,
           publish_global_lifecycle=True,
       ),
   )

Use ``strict=True`` when publish failures should fail the task execution path.
The default is best-effort publishing with warnings on sink failures.

Event Envelope
==============

``QueueEvent`` is a stable JSON-compatible envelope with a camelCase wire
format. Field names on the wire are the camelCase aliases produced by
``msgspec.Struct(rename="camel")``; the Python attribute names stay
snake_case. Null-valued top-level fields are preserved so clients can rely on
a consistent shape for lifecycle, progress, log, and custom events.

Core fields (Python name → wire name):

* ``id`` → ``id``, ``schema_version`` → ``schemaVersion``
* ``type`` → ``type``, ``scope`` → ``scope``, ``scope_key`` → ``scopeKey``
* ``task_id`` → ``taskId``, ``task_name`` → ``taskName``
* ``queue`` → ``queue``, ``worker_id`` → ``workerId``
  (``workerId`` is populated for events emitted from worker-driven
  executions; the default identity is ``worker-{pid}``. Service-driven
  executions without an attached :class:`~litestar_queues.Worker` leave
  it ``null``.)
* ``execution_backend`` → ``executionBackend``,
  ``execution_profile`` → ``executionProfile``
* ``attempt``, ``sequence``, ``level``, ``message``
* ``progress_current`` → ``progressCurrent``,
  ``progress_total`` → ``progressTotal``,
  ``progress_percent`` → ``progressPercent``
* ``actor``, ``entity``, ``payload`` (payload contents are passed through
  verbatim and are NOT renamed)
* ``occurred_at`` → ``occurredAt`` (RFC 3339 UTC with a trailing ``Z``)
* ``event_key`` → ``eventKey``: optional dedup key for
  subscribers; plugin-owned streams deduplicate on this when set and fall back
  to ``id`` otherwise.

Publishing From Tasks
=====================

Tasks can accept ``_task_context`` or use the helper functions that publish
through the current task context:

.. code-block:: python

   from litestar_queues import task
   from litestar_queues.events import publish_task_event, publish_task_log, publish_task_progress


   @task("videos.transcode")
   async def transcode_video(video_id: str) -> None:
       await publish_task_log("Transcode started", payload={"video_id": video_id})
       await publish_task_progress(current=1, total=4, message="Fetched source")
       await publish_task_event("task.event", message="Preview ready", payload={"video_id": video_id})

Producer Facade
===============

Use ``queue_events`` when application handlers or services need to publish queue
events outside a running task context. The dependency injects a
``QueueEventProducer`` backed by the app-scoped publisher:

.. code-block:: python

   from litestar import post
   from litestar.di import NamedDependency
   from litestar_queues.events import QueueEventProducer


   @post("/imports/{import_id:str}/status")
   async def update_import_status(
       import_id: str,
       queue_events: NamedDependency[QueueEventProducer],
   ) -> dict[str, str]:
       await queue_events.channel(f"imports:{import_id}").publish(
           "import.status",
           message="Import queued",
           payload={"import_id": import_id},
       )
       return {"status": "accepted"}

``QueueEventProducer`` exposes scoped handles:

* ``task(task_id)`` for task logs, progress, and custom task events;
* ``queue(name)`` for queue-scoped events;
* ``worker(worker_id)`` for worker-scoped events; and
* ``channel(scope_key)`` for application-defined custom channels.

``QueueService.get_event_producer()`` returns the same facade when code already
has a service instance:

.. code-block:: python

   queue_events = queue_service.get_event_producer()
   await queue_events.task(task_id).progress(current=2, total=5)

External processes can publish without opening a queue backend or worker by
using ``create_event_producer(config)``. Prefer the async context manager so the
configured sink or Channels backend is opened and closed around the publish
window:

.. code-block:: python

   from litestar_queues import QueueConfig
   from litestar_queues.events import EventConfig, create_event_producer


   config = QueueConfig(event=EventConfig(channels_backend=channels))

   async with create_event_producer(config) as queue_events:
       await queue_events.channel("imports:batch-42").publish(
           "import.note",
           payload={"batch_id": "batch-42"},
       )

Buffered Live Delivery
======================

Live delivery is buffered by default for publishers owned by ``QueueService`` or
``QueuePlugin`` when ``EventConfig`` is enabled. A publish call means "accepted
into the queue event publisher"; it does not always mean the live sink has sent
bytes to subscribers yet. Durable event history, when configured, is recorded
before the live event is accepted into the buffer.

The buffer flushes when one of these happens:

* ``EventBufferConfig.buffer_size`` pending live events is reached;
* ``EventBufferConfig.flush_interval`` elapses while the service is open;
* ``QueueEventPublisher.flush_buffer()`` or service shutdown drains the buffer;
* a task helper, task context, or producer handle publishes with
  ``immediate=True``; or
* a terminal task event is published.

Terminal events are always immediate. ``task.completed``, ``task.failed``,
``task.cancelled``, ``task.claim_lost``, and ``task.stale_failed`` flush the
current task's buffered live events first, then publish the terminal event.
This preserves task-scoped ordering without requiring every call site to pass
``immediate=True``.

Configure buffering on ``EventConfig``:

.. code-block:: python

   from litestar_queues import QueueConfig
   from litestar_queues.events import EventBufferConfig, EventConfig


   config = QueueConfig(
       event=EventConfig(
           channels_backend=channels,
           buffer=EventBufferConfig(
               buffer_size=50,
               flush_interval=0.25,
               max_pending=5000,
               overflow="drop_oldest",
           ),
       ),
   )

Set ``EventBufferConfig(enabled=False)`` to restore immediate live publishing:

.. code-block:: python

   config = QueueConfig(
       event=EventConfig(
           channels_backend=channels,
           buffer=EventBufferConfig(enabled=False),
       ),
   )

Overflow behavior is controlled by ``overflow``:

* ``"drop_oldest"`` drops the oldest pending event and accepts the new event;
* ``"drop_newest"`` drops the incoming event;
* ``"block"`` waits for a flush before accepting more events; and
* ``"error"`` raises ``QueueEventBufferFull``.

The default live publish path remains best effort. Set ``strict=True`` when
buffer add, buffer flush, or live sink failures should raise into the caller.

Channels
========

``QueueEventPublisher`` resolves canonical channels from the event scope:

* task channels: ``litestar_queues:task:<task-id>:events``,
* queue channels: ``litestar_queues:queue:<queue>:events``,
* worker channels: ``litestar_queues:worker:<worker-id>:events``,
* global channel: ``litestar_queues:global:events``,
* custom channels: ``litestar_queues:custom:<scope-key>:events``.

Explicit channels passed by task helpers are appended and duplicates are
removed.

Streaming With Litestar Channels
================================

``ChannelsQueueEventSink`` publishes event JSON to an app-owned Channels backend
or plugin. Configure ``EventStreamConfig`` on ``QueueConfig`` when clients need
live updates. The plugin registers WebSocket and SSE endpoints for the configured
scopes and applies the same guards and ``channel_authorizer`` to both transports:

.. code-block:: python

   from litestar_queues import QueueConfig, QueuePlugin
   from litestar_queues.events import EventConfig, EventStreamConfig


   config = QueueConfig(
       event=EventConfig(channels_backend=channels),
       event_stream=EventStreamConfig(
           path="/queues/events",
           scopes={"task", "queue", "worker", "global", "custom"},
           guards=[...],
           channel_authorizer=authorize_channel,
       ),
   )

   plugin = QueuePlugin(config)

The default path is ``/queues/events``. Task-scoped streams are available at
``/queues/events/tasks/{task_id}`` for WebSocket clients and
``/queues/events/sse/tasks/{task_id}`` for SSE clients. Queue, worker, global,
and custom scopes follow the same path pattern.

Subscribers receive at-most-once delivery per ``eventKey`` (or per ``id`` when
no key is set) within a single connection. Set ``QueueEvent(..., event_key=...)``
at publish time when worker-level retries should not double-emit downstream.

Consuming Stream Events
=======================

Queue event streams expose the same event envelope through WebSocket and SSE.
WebSocket streams send one JSON object per message. Heartbeats are JSON frames
with ``{"type": "ping"}``; clients should ignore those frames or use them only
to update a connection liveness timestamp.

The task stream is available to WebSocket clients at
``/queues/events/tasks/{task_id}``. Custom WebSocket clients subscribe at
``/queues/events/custom/{scope_key}``.

The standalone WebSocket example in ``examples/htmx_realtime_websocket`` is a
runnable server-rendered app: a full-screen animated crawl fed by task events,
with a single planet-styled restart button. It registers only the task event
stream:

.. literalinclude:: ../../examples/htmx_realtime_websocket/app.py
   :language: python
   :start-after: # -- docs-app-config-start --
   :end-before: # -- docs-app-config-end --

Its restart endpoint returns a small partial whose element the htmx WebSocket
extension turns into a live socket; replacing that element on the next restart
closes the old socket, so the connection lifecycle equals the swap lifecycle:

.. literalinclude:: ../../examples/htmx_realtime_websocket/app.py
   :language: python
   :start-after: # -- docs-routes-start --
   :end-before: # -- docs-routes-end --

A small adapter parses each JSON frame, ignores ping frames, and appends a
crawl line, cancelling the extension's default HTML swap:

.. literalinclude:: ../../examples/htmx_realtime_websocket/resources/main.ts
   :language: typescript
   :start-after: // docs: websocket-client-start
   :end-before: // docs: websocket-client-end

.. literalinclude:: ../../examples/htmx_realtime_websocket/resources/main.ts
   :language: typescript
   :start-after: // docs: stream-adapter-start
   :end-before: // docs: stream-adapter-end

SSE streams emit named frames whose event name is the queue event type and whose
data is the JSON form of ``QueueEvent.to_dict()``:

.. code-block:: text

   event: task.progress
   data: {"id":"...","type":"task.progress","scope":"task",...}

SSE streams send keepalive comments instead of JSON ping frames. The task stream
is available to SSE clients at ``/queues/events/sse/tasks/{task_id}``. Custom
SSE clients subscribe at ``/queues/events/sse/custom/{scope_key}``.

The standalone SSE example in ``examples/htmx_realtime_sse`` uses the same queue
event configuration, but returns only SSE stream URLs to the browser:

.. literalinclude:: ../../examples/htmx_realtime_sse/app.py
   :language: python
   :start-after: # -- docs-app-config-start --
   :end-before: # -- docs-app-config-end --

.. literalinclude:: ../../examples/htmx_realtime_sse/app.py
   :language: python
   :start-after: # -- docs-routes-start --
   :end-before: # -- docs-routes-end --

A small adapter parses each named SSE frame and appends a crawl line,
cancelling the extension's default HTML swap:

.. literalinclude:: ../../examples/htmx_realtime_sse/resources/main.ts
   :language: typescript
   :start-after: // docs: sse-client-start
   :end-before: // docs: sse-client-end

.. literalinclude:: ../../examples/htmx_realtime_sse/resources/main.ts
   :language: typescript
   :start-after: // docs: stream-adapter-start
   :end-before: // docs: stream-adapter-end

Both examples use the same task body, which publishes heartbeat details,
progress, logs, and a terminal completion event:

.. literalinclude:: ../../examples/htmx_realtime_websocket/app.py
   :language: python
   :start-after: # -- docs-task-start --
   :end-before: # -- docs-task-end --

The page shell publishes htmx on ``window``, registers the Litestar Vite HTMX
helper, and dynamically imports the stream extension after the global exists. A
single restart button carries the ``hx-post`` directly, and a muted corner
caption uses ``hx-ext="litestar"`` for ``ls-*`` JSON templates:

.. literalinclude:: ../../examples/htmx_realtime_websocket/resources/main.ts
   :language: typescript
   :start-after: // docs: htmx-extension-start
   :end-before: // docs: htmx-extension-end

.. literalinclude:: ../../examples/htmx_realtime_websocket/templates/index.html
   :language: html
   :start-after: <!-- docs: template-start -->
   :end-before: <!-- docs: template-end -->

.. literalinclude:: ../../examples/htmx_realtime_sse/templates/index.html
   :language: html
   :start-after: <!-- docs: template-start -->
   :end-before: <!-- docs: template-end -->

Backend-specific WebSocket copies are available under
``examples/htmx_realtime_websocket_sqlspec``,
``examples/htmx_realtime_websocket_advanced_alchemy``,
``examples/htmx_realtime_websocket_redis``, and
``examples/htmx_realtime_websocket_valkey``.

Backend-specific SSE copies are available under
``examples/htmx_realtime_sse_sqlspec``,
``examples/htmx_realtime_sse_advanced_alchemy``,
``examples/htmx_realtime_sse_redis``, and
``examples/htmx_realtime_sse_valkey``. The SQLSpec and Advanced Alchemy copies
use local ``aiosqlite`` files; Redis and Valkey read their connection URL from
environment variables.

Subscriber backpressure is owned by the configured Litestar Channels plugin or
backend. The default Channels subscriber backlog is unbounded. For browser
streams, configure a bounded backlog and drop older messages in favor of newer
state updates:

.. code-block:: python

   from litestar.channels import ChannelsPlugin


   channels = ChannelsPlugin(
       backend=channels_backend,
       arbitrary_channels_allowed=True,
       subscriber_max_backlog=1000,
       subscriber_backlog_strategy="dropleft",
   )

Transport Recommendations
=========================

Choose the Channels backend based on whether browser clients need broadcast
fan-out, replay, or durable claim semantics:

.. list-table::
   :header-rows: 1

   * - Transport
     - Semantics
     - Recommended Use
   * - Redis Channels pub/sub backend
     - Broadcast fan-out across processes, ephemeral delivery, no history.
     - Live-only browser fan-out across multiple web processes.
   * - Redis Streams Channels backend
     - Broadcast fan-out with stream history through ``XADD``/``XRANGE`` and
       ``MAXLEN``.
     - Browser fan-out when replay or subscriber backlog is required.
   * - ``SQLSpecChannelsBackend`` over sqlspec events ``listen_notify``
       (``AsyncpgEventsBackend``, ``backend_name="listen_notify"``)
     - Broadcast fan-out across processes through ``pg_notify``. Delivery is
       ephemeral: ``ack``/``nack`` are no-ops and no history is stored. The
       payload is sent inline through PostgreSQL notify, so it is subject to the
       roughly 8 KB ``pg_notify`` cap.
     - SQLSpec applications that need live multi-replica browser fan-out and can
       keep payloads small. Use ``EventConfig.max_payload_bytes`` to chunk larger
       events.
   * - ``SQLSpecChannelsBackend`` over sqlspec events
       ``listen_notify_durable`` (``AsyncpgHybridEventsBackend``) or
       ``table_queue`` (``AsyncTableEventQueue``)
     - Durable table-backed events with claim, lease, and ack. Rows move from
       ``pending`` to ``leased`` to ``acked`` under ``FOR UPDATE SKIP LOCKED``.
       This is competing-consumer delivery: each event is claimed and acked by
       exactly one consumer.
     - Durable worker-style consumers. A single web process can still fan out to
       its local browser subscribers through ``ChannelsPlugin``, but multiple
       web processes subscribed to the same channel will compete and split
       events. Use a distinct consumer/channel per process, or use
       ``listen_notify`` or Redis for multi-replica browser fan-out. The hybrid
       stores payloads in the table and notifies only ``event_id``, avoiding the
       PostgreSQL notify payload cap.

Litestar's own asyncpg and psycopg Channels backends are useful for simple
deployments, but they open a fresh connection per publish, loop
``SELECT pg_notify`` once per channel, and raise ``NotImplementedError`` for
history reads. Buffering amortizes the connect storm, but it does not add
history. For production fan-out, prefer Redis Channels or
``SQLSpecChannelsBackend`` with the semantics above.

Durable Event History
=====================

Live event sinks are delivery transports. They publish events to memory,
Channels, or a custom realtime system and should not be used as durable storage.
Queryable event history belongs to the configured queue backend so task state
and task event history share the same database, schema lifecycle, and deployment
boundary.

Backend-managed event history is configured separately from
``EventConfig.sink``. When durable history is enabled, events can be
recorded even if live delivery is disabled.

SQLSpec provides backend-managed history through the same database lifecycle as
the queue table:

.. code-block:: python

   from litestar_queues import QueueConfig
   from litestar_queues.events import EventLogConfig
   from litestar_queues.backends.sqlspec import SQLSpecBackendConfig


   config = QueueConfig(
       queue_backend=SQLSpecBackendConfig(config=sqlspec_config),
       event_log=EventLogConfig(enabled=True),
   )

By default SQLSpec stores history in ``<queue_table>_event_log``. For example,
``litestar_queue_task`` uses ``litestar_queue_task_event_log``. Override the
table with ``SQLSpecBackendConfig.event_log_table_name`` when an adopter needs a
specific table name or compatibility view.

Testing Events
==============

Use ``InMemoryQueueEventSink`` for tests:

.. code-block:: python

   from litestar_queues import QueueConfig
   from litestar_queues.events import (
       EventBufferConfig,
       EventConfig,
       InMemoryQueueEventSink,
   )


   sink = InMemoryQueueEventSink()
   config = QueueConfig(
       event=EventConfig(
           sink=sink,
           buffer=EventBufferConfig(enabled=False),
       ),
   )

   # After task execution:
   assert [event.type for event in sink.events] == ["task.started", "task.completed"]

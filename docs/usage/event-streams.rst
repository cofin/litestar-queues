===================
SSE and WebSockets
===================

The plugin can add task, queue, worker, global, and custom stream routes under
``/queues/events``:

.. code-block:: python

   from litestar.channels import ChannelsPlugin
   from litestar.channels.backends.memory import MemoryChannelsBackend
   from litestar.connection import ASGIConnection

   from litestar_queues import QueueConfig
   from litestar_queues.events import (
       EventDeliveryConfig,
       EventStreamConfig,
       QueueEventsConfig,
       QueueEventScope,
   )

   channels = ChannelsPlugin(
       backend=MemoryChannelsBackend(history=200),
       arbitrary_channels_allowed=True,
   )


   async def authorize_channel(
       connection: ASGIConnection, scope: QueueEventScope, key: "str | None"
   ) -> bool:
       """Allow a client to subscribe only to its own tasks."""
       return key is not None and key in connection.session.get("task_ids", ())


   queue_config = QueueConfig(
       events=QueueEventsConfig(
           channels=channels,
           delivery=EventDeliveryConfig(),
           stream=EventStreamConfig(
               transports={"sse", "websocket"},
               scopes={"task", "queue"},
               channel_authorizer=authorize_channel,
           ),
       ),
   )

``MemoryChannelsBackend`` is per-process. Use a Redis, Valkey, or PostgreSQL
Channels backend when more than one process serves streams. The authorizer
above reads Litestar's session, so it assumes session middleware is installed;
any callable with that signature works, including one that queries your own
permission store.

Use SSE for server-to-browser updates with automatic browser reconnection. Use
WebSockets when the connection also needs bidirectional application messages.
Both deliver the same JSON ``QueueEvent`` envelope.

The default path follows :attr:`~litestar_queues.QueueConfig.namespace`: the
legacy namespace uses ``/queues/events``, while
``QueueConfig(namespace="myapp")`` uses ``/myapp/events``. Set
``EventStreamConfig(path="/events")`` when the application owns an explicit
path; explicit paths are never rewritten.

When ``QueueEventsConfig.channels`` is set, generated stream routes use that
same Channels source even if the application registers another
``ChannelsPlugin``. Without an explicit source, the routes use the registered
plugin. Event publishing and route registration remain separate settings so a
web process can serve events published by another process.

Envelope and delivery semantics
===============================

Top-level JSON fields use camel case, including ``taskId``,
``workerId``, ``scopeKey``, ``progressCurrent``, ``occurredAt``, and
``eventKey``. Null-valued keys are preserved; user-owned ``payload`` keys are
not renamed. Consumers should deduplicate by ``eventKey`` when present and by
``id`` otherwise.

A slow client may miss best-effort events, depending on the Channels backend
and its backlog policy, so query :doc:`event-history` when a client must replay
events. See :ref:`live-delivery-vs-history` for what a live stream does and
does not guarantee.

Authorization
=============

Application-level guards apply to the generated routes. Use ``guards`` to add
stream-specific protection and ``channel_authorizer`` to check each scope and
key. The ``scopes`` setting controls which route families are registered; it
does not authorize access. Never accept a task ID or queue name from a client
without checking that the tenant and user may access it.

For an intentionally public stream, set ``unauthenticated_access="allow"`` to
acknowledge that choice and suppress the startup warning. Set
``include_in_schema=True`` separately when the generated routes should appear
in the OpenAPI schema.

Canonical runnable sources
==========================

The memory examples are the complete examples. Each backend copy changes only
the queue-storage setup and keeps the same frontend behavior.

WebSocket application:

.. literalinclude:: ../../examples/htmx_realtime_websocket/app.py
   :language: python
   :caption: examples/htmx_realtime_websocket/app.py

WebSocket frontend. All ten examples share one Vite project under
``examples/shared``, with an entry point per transport:

.. literalinclude:: ../../examples/shared/resources/main-ws.ts
   :language: typescript
   :caption: examples/shared/resources/main-ws.ts

.. literalinclude:: ../../examples/shared/templates/partials/stream_mount_ws.html
   :language: html+jinja
   :caption: examples/shared/templates/partials/stream_mount_ws.html

SSE application:

.. literalinclude:: ../../examples/htmx_realtime_sse/app.py
   :language: python
   :caption: examples/htmx_realtime_sse/app.py

SSE frontend:

.. literalinclude:: ../../examples/shared/resources/main-sse.ts
   :language: typescript
   :caption: examples/shared/resources/main-sse.ts

.. literalinclude:: ../../examples/shared/templates/partials/stream_mount_sse.html
   :language: html+jinja
   :caption: examples/shared/templates/partials/stream_mount_sse.html

The page markup both transports render is shared too:

.. literalinclude:: ../../examples/shared/templates/index.html
   :language: html+jinja
   :caption: examples/shared/templates/index.html

Backend variants
================

The gallery includes these runnable source families:

* ``examples/htmx_realtime_websocket_sqlspec`` and
  ``examples/htmx_realtime_sse_sqlspec``
* ``examples/htmx_realtime_websocket_advanced_alchemy`` and
  ``examples/htmx_realtime_sse_advanced_alchemy``
* ``examples/htmx_realtime_websocket_redis`` and
  ``examples/htmx_realtime_sse_redis``
* ``examples/htmx_realtime_websocket_valkey`` and
  ``examples/htmx_realtime_sse_valkey``

The browser test clicks Restart, observes the enqueue request and page updates,
then waits for the final event. A curl stream cannot prove that the HTMX
extension starts or that the browser opens and closes the connection correctly.

===================
SSE and WebSockets
===================

The plugin can add task, queue, worker, global, and custom stream routes under
``/queues/events``:

.. code-block:: python

   from litestar_queues import QueueConfig
   from litestar_queues.events import EventConfig, EventStreamConfig

   queue_config = QueueConfig(
       event=EventConfig(channels_backend=channels_backend),
       event_stream=EventStreamConfig(
           sse=True,
           websocket=True,
           scopes={"task", "queue"},
           channel_authorizer=authorize_channel,
       ),
   )

Use SSE for server-to-browser updates with automatic browser reconnection. Use
WebSockets when the connection also needs bidirectional application messages.
Both deliver the same JSON ``QueueEvent`` envelope.

Envelope and delivery semantics
===============================

Top-level JSON fields use camel case, including ``taskId``,
``workerId``, ``scopeKey``, ``progressCurrent``, ``occurredAt``, and
``eventKey``. Null-valued keys are preserved; user-owned ``payload`` keys are
not renamed. Consumers should deduplicate by ``eventKey`` when present and by
``id`` otherwise.

Live streams deliver events; they do not store tasks. Keepalives keep an idle
connection open, but they do not prove that a task is healthy. A slow client
may miss best-effort events, depending on the Channels backend and its backlog
policy. Query :doc:`event-history` when a client must replay events.

Authorization
=============

Use ``guards`` to protect an entire route. Use ``channel_authorizer`` to check
each scope and key. Never accept a task ID or queue name from a client without
checking that the tenant and user may access it. Set ``include_in_schema=True``
only when these generated routes should be public.

Canonical runnable sources
==========================

The memory examples are the complete examples. Each backend copy changes only
the queue-storage setup and keeps the same frontend behavior.

WebSocket application:

.. literalinclude:: ../../examples/htmx_realtime_websocket/app.py
   :language: python
   :caption: examples/htmx_realtime_websocket/app.py

WebSocket frontend:

.. literalinclude:: ../../examples/htmx_realtime_websocket/resources/main.ts
   :language: typescript
   :caption: examples/htmx_realtime_websocket/resources/main.ts

.. literalinclude:: ../../examples/htmx_realtime_websocket/templates/index.html
   :language: html+jinja
   :caption: examples/htmx_realtime_websocket/templates/index.html

SSE application:

.. literalinclude:: ../../examples/htmx_realtime_sse/app.py
   :language: python
   :caption: examples/htmx_realtime_sse/app.py

SSE frontend:

.. literalinclude:: ../../examples/htmx_realtime_sse/resources/main.ts
   :language: typescript
   :caption: examples/htmx_realtime_sse/resources/main.ts

.. literalinclude:: ../../examples/htmx_realtime_sse/templates/index.html
   :language: html+jinja
   :caption: examples/htmx_realtime_sse/templates/index.html

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

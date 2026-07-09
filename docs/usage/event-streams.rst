===================
SSE and WebSockets
===================

The plugin can register task, queue, worker, global, and custom stream routes
under ``/queues/events``:

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

Top-level fields use camel case on the wire, including ``taskId``,
``workerId``, ``scopeKey``, ``progressCurrent``, ``occurredAt``, and
``eventKey``. Null-valued keys are preserved; user-owned ``payload`` keys are
not renamed. Consumers should deduplicate by ``eventKey`` when present and by
``id`` otherwise.

Live streams are delivery paths, not durable work queues. Keepalives preserve
idle connections; they do not confirm task health. Slow clients can miss
best-effort events depending on the Channels backend and backlog policy. Query
:doc:`event-history` when replay is required.

Authorization
=============

Use ``guards`` for route-wide access and ``channel_authorizer`` for each scope
and key. Never accept an arbitrary task ID or queue name from a client without
checking tenant and user ownership. Set ``include_in_schema=True`` only when
the generated route surface is intended to be public.

Canonical runnable sources
==========================

The memory examples are the long-form authority. The backend copies change
persistence wiring while preserving the same frontend contract.

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

Browser acceptance clicks the restart control, observes the enqueue request,
watches DOM updates, and waits for the terminal event. A curl stream alone
does not prove the HTMX extension or browser connection lifecycle.

================
Examples gallery
================

Start with a memory example. It runs the web app, queue worker, and live
Channels delivery in one process. Choose a backend variant only after the
transport is working.

.. grid:: 1 1 2 2
   :gutter: 2
   :padding: 0

   .. grid-item-card:: Memory WebSocket
      :link: https://github.com/cofin/litestar-queues/tree/main/examples/htmx_realtime_websocket
      :link-type: url

      HTMX WebSocket updates with memory queue persistence and memory Channels.

   .. grid-item-card:: Memory SSE
      :link: https://github.com/cofin/litestar-queues/tree/main/examples/htmx_realtime_sse
      :link-type: url

      HTMX Server-Sent Events with memory queue persistence and memory Channels.

   .. grid-item-card:: SQL backends
      :link: ../usage/backends
      :link-type: doc

      Run the SQLSpec or Advanced Alchemy copies with SQLite, then adapt schema ownership.

   .. grid-item-card:: Redis and Valkey
      :link: ../usage/backends/redis-valkey
      :link-type: doc

      Run local copies or opt into shared Channels and a standalone worker.

Every variant has its own README, application module, templates, frontend
source, and asset commands. The memory apps are canonical for concepts; backend
copies demonstrate persistence wiring rather than repeating the full tutorial.

.. note::

   Selecting Redis or Valkey queue persistence does not select browser fan-out.
   The default examples use process-local Channels. Set the documented shared
   Channels and standalone-worker environment variables together.

See :doc:`../usage/event-streams` for the source imports and delivery contract,
or browse the complete `examples directory
<https://github.com/cofin/litestar-queues/tree/main/examples>`_.

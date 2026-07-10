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
source, and asset commands. The memory apps are the main examples. The backend
copies show only how persistence changes.

.. note::

   Selecting Redis or Valkey for queue storage does not configure browser event
   delivery. By default, the examples use Channels in one process. To run a
   separate worker, set both the shared-Channels and standalone-worker
   environment variables described in the example README.

See :doc:`../usage/event-streams` for the source imports and delivery contract,
or browse the complete `examples directory
<https://github.com/cofin/litestar-queues/tree/main/examples>`_.

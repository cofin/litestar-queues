.. title:: Litestar Queues

.. meta::
   :description: Beginner-first task queues, workers, schedules, persistence backends, and realtime events for Litestar.
   :keywords: Litestar, task queue, worker, background job, Redis, Valkey, SQLSpec, Advanced Alchemy

.. container:: title-with-logo

   .. raw:: html

      <h1 class="brand-text" aria-label="Litestar Queues">Litestar Queues</h1>

Define background work with a typed decorator, add it to a queue through
Litestar, and let a managed worker run it. Start in one process. Add persistent
storage, separate workers, or live task events as the application grows.

.. toctree::
   :hidden:
   :titlesonly:
   :caption: Documentation

   getting_started/index
   usage/concepts
   usage/index
   examples/index
   reference/index
   contributing/index

.. grid:: 1 1 2 3
   :padding: 0
   :gutter: 2

   .. grid-item-card:: Start here
      :link: getting_started/index
      :link-type: doc

      Install the package, copy one complete application, and enqueue a task.

   .. grid-item-card:: Concepts
      :link: usage/concepts
      :link-type: doc

      Learn how task records, storage, workers, execution, and events fit together.

   .. grid-item-card:: How-to guides
      :link: usage/index
      :link-type: doc

      Define tasks, inspect results, operate workers, and choose production options.

   .. grid-item-card:: Examples
      :link: examples/index
      :link-type: doc

      Run visual WebSocket and SSE examples with memory or persistent backends.

   .. grid-item-card:: Reference
      :link: reference/index
      :link-type: doc

      Browse generated API documentation grouped by subsystem.

   .. grid-item-card:: Developers
      :link: contributing/index
      :link-type: doc

      Test changes, maintain examples, and audit documentation.

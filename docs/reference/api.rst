=============
Reference Map
=============

The API reference is split by subsystem so each page stays focused and matches
the package layout.

.. grid:: 1 1 2 2
   :gutter: 2
   :padding: 0

   .. grid-item-card:: Core API
      :link: core
      :link-type: doc

      Configuration, service, task, model, worker, plugin, and exception APIs.

   .. grid-item-card:: Queue Backends
      :link: backends
      :link-type: doc

      Queue backend registry, base protocol, memory, SQLSpec, Advanced Alchemy,
      Redis, and Valkey modules.

   .. grid-item-card:: Execution Backends
      :link: execution
      :link-type: doc

      Execution backend registry, immediate execution, local workers, and Cloud
      Run execution modules.

   .. grid-item-card:: Events
      :link: events
      :link-type: doc

      Queue event models, publishers, context helpers, sinks, channels, and
      Litestar Channels integration.

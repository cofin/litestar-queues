=============
API Reference
=============

Auto-generated documentation from source code docstrings, grouped by subsystem.

Core
====

.. grid:: 1 1 2 2
   :gutter: 2
   :padding: 0

   .. grid-item-card:: Core API
      :link: core
      :link-type: doc

      Configuration, service, task, model, worker, plugin, and exception APIs.

   .. grid-item-card:: Reference Map
      :link: api
      :link-type: doc

      A quick index for the grouped API reference pages.

Backends
========

.. grid:: 1 1 2 2
   :gutter: 2
   :padding: 0

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

Realtime
========

.. grid:: 1 1 2 2
   :gutter: 2
   :padding: 0

   .. grid-item-card:: Events
      :link: events
      :link-type: doc

      Queue event envelopes, publishers, context helpers, sinks, channel names,
      and Litestar Channels integration.

.. toctree::
   :hidden:
   :maxdepth: 1

   core
   backends
   execution
   events
   api

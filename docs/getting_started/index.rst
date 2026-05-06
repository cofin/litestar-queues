===============
Getting Started
===============

.. grid:: 1
   :padding: 0
   :gutter: 2

   .. grid-item-card::

      **Start here.** Litestar Queues keeps the application-facing API small:
      decorate a callable, configure queue and execution backends, then enqueue
      records through ``QueueService``.

Choose a Guide
==============

.. grid:: 1 1 2 3
   :gutter: 2
   :padding: 0

   .. grid-item-card:: Introduction
      :link: introduction
      :link-type: doc

      Understand the core queue, execution, worker, and event concepts.

   .. grid-item-card:: Installation
      :link: installation
      :link-type: doc

      Install the core package and only the optional backend extras you need.

   .. grid-item-card:: Quickstart
      :link: quickstart
      :link-type: doc

      Register a task, attach the Litestar plugin, and enqueue work.

.. toctree::
   :hidden:

   introduction
   installation
   quickstart

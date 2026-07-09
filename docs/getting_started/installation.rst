============
Installation
============

Install the core package for task registration, the Litestar plugin, memory
queue persistence, and local or immediate execution:

.. code-block:: bash

   pip install litestar-queues

Optional extras
===============

Install only the integration your application uses:

.. list-table::
   :header-rows: 1

   * - Command
     - Adds
   * - ``pip install "litestar-queues[sqlspec]"``
     - SQLSpec queue persistence; install the SQLSpec driver for your database separately.
   * - ``pip install "litestar-queues[advanced-alchemy]"``
     - Advanced Alchemy and SQLAlchemy queue persistence.
   * - ``pip install "litestar-queues[redis]"``
     - Redis queue persistence and worker wakeups.
   * - ``pip install "litestar-queues[valkey]"``
     - Valkey queue persistence and worker wakeups.
   * - ``pip install "litestar-queues[cloudrun]"``
     - Google Cloud Run Jobs execution.
   * - ``pip install "litestar-queues[otel]"``
     - OpenTelemetry instrumentation.
   * - ``pip install "litestar-queues[prometheus]"``
     - Prometheus metrics.
   * - ``pip install "litestar-queues[examples]"``
     - Dependencies used by the runnable HTMX examples.

Extras install libraries, but they do not choose your architecture. See
:doc:`../usage/backends` after completing the :doc:`quickstart`.

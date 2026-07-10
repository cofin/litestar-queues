============
Installation
============

Install the core package. It provides task registration, the Litestar plugin,
in-memory queue storage, and local or immediate execution:

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

An extra installs the libraries for an integration. It does not configure that
integration or choose where tasks run. Complete the :doc:`quickstart`, then
see :doc:`../usage/backends`.

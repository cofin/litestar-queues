============
Installation
============

Install the core package when you need the in-memory queue backend, immediate
execution, local workers, task registration, and Litestar plugin lifecycle
support:

.. code-block:: bash

   pip install litestar-queues

Optional Backends
=================

Install backend extras only for the integrations an application uses:

.. list-table::
   :header-rows: 1

   * - Extra
     - Enables
     - Notes
   * - ``sqlspec``
     - SQLSpec queue persistence
     - Installs SQLSpec with the Aiosqlite extra. Install additional SQLSpec
       adapter drivers in the application when you use them.
   * - ``advanced-alchemy``
     - Advanced Alchemy queue persistence
     - Uses application-owned SQLAlchemy configuration or session makers.
   * - ``redis``
     - Redis queue persistence
     - Imports the Redis client only when opening the Redis backend without an
       injected client.
   * - ``valkey``
     - Valkey queue persistence
     - Imports the Valkey client only when opening the Valkey backend without an
       injected client.
   * - ``cloudrun``
     - Cloud Run execution
     - Dispatches persisted queue records to Cloud Run Jobs.
   * - ``otel``
     - OpenTelemetry queue traces and metrics
     - Adds producer, consumer, worker, dispatch, and reconcile telemetry.
   * - ``prometheus``
     - Prometheus queue metrics
     - Exposes bounded queue-domain counters and histograms.

.. code-block:: bash

   pip install litestar-queues[sqlspec]
   pip install litestar-queues[advanced-alchemy]
   pip install litestar-queues[redis]
   pip install litestar-queues[valkey]
   pip install litestar-queues[cloudrun]
   pip install litestar-queues[otel]
   pip install litestar-queues[prometheus]

Extras can be combined when a deployment needs multiple integrations:

.. code-block:: bash

   pip install "litestar-queues[sqlspec,cloudrun]"
   pip install "litestar-queues[otel,prometheus]"

Optional Import Boundaries
==========================

The package registers optional backend names without importing their external
client libraries from package root or shared backend import paths. Opening an
optional backend requires either the matching extra or an injected client/config
object supplied by the application.

This keeps a core installation usable for tests and simple local workers while
allowing production deployments to choose SQLSpec adapters, Advanced Alchemy,
Redis, Valkey, or Cloud Run independently.

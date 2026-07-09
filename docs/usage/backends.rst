===============
Choose backends
===============

Make five independent decisions. Most applications need only the first two;
the others add latency improvements or user-facing delivery.

.. list-table::
   :header-rows: 1
   :widths: 18 18 16 18 18 18

   * - Need
     - Queue persistence
     - Execution placement
     - Worker wakeup strategy
     - User-facing event stream
     - Next guide
   * - One process or tests
     - ``memory`` (process-local)
     - ``local`` worker or inline ``immediate``
     - Polling/in-process hint
     - Memory Channels (process-local)
     - :doc:`../getting_started/quickstart`
   * - SQLSpec application
     - ``SQLSpecBackendConfig``
     - ``local`` or Cloud Run
     - SQLSpec transport when supported, otherwise polling
     - Configure Channels separately
     - :doc:`backends/sqlspec`
   * - SQLAlchemy application
     - ``AdvancedAlchemyBackendConfig``
     - ``local`` or Cloud Run
     - Optional PostgreSQL hint, otherwise polling
     - Configure Channels separately
     - :doc:`backends/advanced-alchemy`
   * - Redis infrastructure
     - ``RedisBackendConfig``
     - ``local`` or Cloud Run
     - Redis pub/sub hint
     - Optional Redis Channels
     - :doc:`backends/redis-valkey`
   * - Valkey infrastructure
     - ``ValkeyBackendConfig``
     - ``local`` or Cloud Run
     - Valkey pub/sub hint
     - Optional Valkey-compatible Channels
     - :doc:`backends/redis-valkey`

The queue backend stores records. ``local`` means worker-process execution,
``immediate`` means inline execution, and Cloud Run is remote execution—not
queue storage. A backend notification wakes workers; it does not fan task
events out to browsers.

Install extras
==============

.. list-table::
   :header-rows: 1

   * - Integration
     - Install extra
     - Shared scope
   * - Memory
     - Core package
     - Current Python process only
   * - SQLSpec
     - ``litestar-queues[sqlspec]`` plus a SQLSpec driver
     - Shared database
   * - Advanced Alchemy
     - ``litestar-queues[advanced-alchemy]``
     - Shared database
   * - Redis
     - ``litestar-queues[redis]``
     - Shared Redis service
   * - Valkey
     - ``litestar-queues[valkey]``
     - Shared Valkey service
   * - Cloud Run execution
     - ``litestar-queues[cloudrun]``
     - Shared persistent queue required

Event-history support
=====================

.. list-table::
   :header-rows: 1

   * - Queue backend
     - History support
     - Ownership
   * - Memory
     - Supported, bounded and ephemeral
     - ``EventLogConfig.max_records`` caps records in the process.
   * - SQLSpec
     - Supported
     - Queue schema, migrations, and SQLSpec session lifecycle.
   * - Advanced Alchemy
     - Supported
     - Application-owned model and schema migrations.
   * - Redis / Valkey
     - Supported
     - Operator controls service durability, retention, backups, and cleanup.

History is not a live sink. See :doc:`event-history` and :doc:`event-streams`.

Topology and security
=====================

.. list-table::
   :header-rows: 1

   * - Topology
     - Queue records
     - Live events
     - Security boundary
   * - Memory examples
     - Process-local memory
     - ``MemoryChannelsBackend`` in the same process
     - Local demo; do not expose as a multi-replica service.
   * - Separate Redis/Valkey worker
     - Shared Redis or Valkey
     - Explicit shared Channels backend and distinct key prefix
     - Authenticate the service, isolate prefixes, and authorize stream routes.
   * - SQL/AA workers
     - Shared database
     - Separately configured Channels transport
     - Protect database credentials and authorize subscriber scopes.

Continue with :doc:`workers`, :doc:`worker-wakeups`, or the focused backend
guide selected above.

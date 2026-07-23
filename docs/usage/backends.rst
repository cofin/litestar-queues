===============
Choose backends
===============

Make these five choices separately. Most applications need only task storage
and execution placement. The other choices can reduce worker delay or deliver
events to users.

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
     - Native transport by default when supported, otherwise polling
     - Configure Channels separately
     - :doc:`backends/sqlspec`
   * - SQLAlchemy application
     - ``SQLAlchemyBackendConfig``
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

The queue backend stores task records. ``local`` runs work in a worker process.
``immediate`` runs it during the enqueue call. Cloud Run runs it remotely; it
does not store the queue. Backend notifications wake workers, but they do not
send task events to browsers.

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
     - ``EventHistoryConfig.memory_capacity`` caps records in the process.
   * - SQLSpec
     - Supported
     - The app owns the queue schema and migrations; SQLSpec manages the sessions.
   * - Advanced Alchemy
     - Supported
     - The app owns the model and migrations.
   * - Redis / Valkey
     - Supported
     - You choose how long records stay and whether to back them up.

Event history saves records for later queries. It does not deliver live events.
See :doc:`event-history` and :doc:`event-streams`.

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

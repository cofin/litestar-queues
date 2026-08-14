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

Worker-wakeup capability matrix
================================

This matrix is checked against the runtime's explicit SQLSpec adapter mapping,
canonical transport set, and Advanced Alchemy driver set:

.. transport-capability-matrix-start

.. list-table::
   :header-rows: 1
   :widths: 28 18 22 10 28

   * - Backend or adapter
     - Enabling setting
     - Effective strategy
     - Durable wakeup
     - Ownership

   * - Memory
     - Always
     - asyncio-event
     - No
     - Process-local hint
   * - SQLSpec: asyncpg, psycopg, psqlpy
     - Default
     - notify_queue
     - Yes
     - Durable queue plus PostgreSQL push
   * - SQLSpec: DuckDB
     - Default
     - poll_queue
     - Yes
     - Durable embedded queue
   * - SQLSpec: other adapters
     - Default
     - Polling
     - N/A
     - Durable task-state polling
   * - SQLSpec: Oracle
     - Explicit only
     - aq or txeventq
     - Yes
     - Application-provisioned Oracle queue
   * - Advanced Alchemy: asyncpg, psycopg
     - worker_wakeups=True
     - postgres-listen-notify
     - No
     - Transient marker
   * - Advanced Alchemy: other drivers
     - Any
     - Polling
     - N/A
     - Durable task-state polling
   * - Redis
     - Default
     - Redis pub/sub
     - No
     - Transient marker
   * - Valkey
     - Default
     - Valkey pub/sub
     - No
     - Transient marker

.. transport-capability-matrix-end

``Durable wakeup`` describes the notification transport, not the queue record.
Queue records remain authoritative in every row.

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

Adding a wakeup or control transport
====================================

The rows above are what the shipped backends provide. If you are writing a new
queue backend, two optional hooks on the backend base class carry best-effort
hints, and both are lossy by contract — a dropped hint costs latency, never
correctness:

.. list-table::
   :header-rows: 1

   * - Hook pair
     - Purpose
   * - ``notify_new_task`` / ``wait_for_wakeups``
     - Tell an idle worker that new work exists, so it stops waiting early.
   * - ``notify_worker_control`` / ``wait_for_worker_control``
     - Tell the owning worker to reconcile now, used by running cancellation.

A backend that implements neither inherits pure polling and stays correct: the
worker reconciles durable queue and task state on a fixed cadence regardless.
No wakeup or cancellation outcome may depend on a hint arriving.

Continue with :doc:`workers` or the focused backend guide selected above.

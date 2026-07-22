==========================
Redis and Valkey backends
==========================

Redis and Valkey store queue records in a shared service and use pub/sub hints
to wake workers. Choose the client that matches the service your application
operates.

.. code-block:: bash

   pip install "litestar-queues[redis]"
   # or: pip install "litestar-queues[valkey]"

.. code-block:: python

   from litestar_queues import QueueConfig
   from litestar_queues.backends.redis import RedisBackendConfig

   queue_config = QueueConfig(
       queue_backend=RedisBackendConfig(
           url="redis://localhost:6379/0",
           key_prefix="myapp:queues",
           notifications=True,
       ),
       execution_backend="local",
       in_app_worker=False,
   )

Use ``ValkeyBackendConfig`` from ``litestar_queues.backends.valkey`` for
Valkey. Both accept the same URL-shaped connection syntax, but Valkey uses the
Valkey client and does not require Redis as an import side effect.

.. note:: Performance

   Install ``hiredis`` alongside ``redis`` for a compiled response parser:
   ``pip install "redis[hiredis]"``, or add ``hiredis`` to your dependencies
   directly. redis-py uses it automatically when it is importable; no
   configuration change is required. The backend requires a client that
   supports pipelines and ``EVAL``; both redis-py and valkey-py provide this.

Payloads and key isolation
==========================

Task arguments, keyword arguments, metadata, results, and errors must be JSON
serializable. Give each application and environment a distinct ``key_prefix``;
do not use ``FLUSHALL`` for test cleanup on shared infrastructure.

.. _redis-maintenance-index-upgrade:

Maintenance index upgrade
=========================

Positive maintenance limits use versioned indexes so each run examines only
the requested number of records. A new or empty ``key_prefix`` initializes
these indexes automatically.

When upgrading a populated prefix created before this release, bounded
maintenance fails closed until the indexes are rebuilt. Stop every Redis or
Valkey queue writer using that prefix, then run this once with the configured
backend:

.. code-block:: python

   rebuilt = await backend.rebuild_maintenance_indexes()

The return value is the number of queue records examined. The rebuild is
explicit, unbounded, and idempotent, so an interrupted call is safe to repeat.
Restart the writers only after it completes.

Batch claiming
==============

Redis and Valkey deliberately do not advertise native batch claiming
(``capabilities.supports_batch_claim`` is ``False``). Workers claim tasks by
looping the exclusive single-task ``claim_next`` primitive, which preserves both
priority ordering and single-owner semantics.

A bounded atomic ``claim_many`` is not possible on the current storage layout:
the ready set is a sorted set keyed by due time, while claim eligibility also
requires priority ordering. An atomic implementation would either scan every due
task (unbounded) or inspect only a due-time prefix and thereby change fairness.
Supporting it correctly requires a separate ready-by-priority index migration,
which is intentionally out of scope here.

Worker wakeups
==============

``notifications=True`` publishes non-durable worker hints. Workers still poll
the stored queue state. ``notification_channel`` and queue key prefixes belong
to queue operations, not browser Channels.

Event history and live delivery
===============================

Backend-managed event history is supported. You choose how long Redis or
Valkey keeps history, what it backs up, and when it removes old records. The
library cannot make an otherwise temporary service durable.

A Redis or Valkey queue backend does not automatically send events to browsers.
For standalone workers or multiple web processes, configure a shared Channels
backend with its own key prefix. Redis pub/sub is temporary; Redis Streams can
keep a backlog. Protect stream access at the Litestar route.

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

   from litestar_queues import QueueConfig, WorkerConfig
   from litestar_queues.backends.redis import RedisBackendConfig

   queue_config = QueueConfig(
       queue_backend=RedisBackendConfig(
           url="redis://localhost:6379/0",
           key_prefix="myapp:queues",
           worker_wakeups=True,
       ),
       execution_backend="local",
       worker=WorkerConfig(placement="external"),
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

Typical pickup time
===================

On a local development machine with an idle worker already running, 95 out of
100 tasks started within about **4.3 ms with Redis** and **4.6 ms with Valkey**.
These are useful expectations, not guarantees: network distance, machine load,
task volume, and service configuration can all increase pickup time.

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
Valkey queue writer using that prefix, then run the rebuild once as a
standalone script. ``queue_config`` is the configuration from the example
above:

.. code-block:: python

   import asyncio

   from litestar_queues import QueueService


   async def rebuild_indexes() -> None:
       async with QueueService(queue_config) as queue_service:
           rebuilt = await queue_service.get_queue_backend().rebuild_maintenance_indexes()
           print(f"reindexed {rebuilt} queue records")


   asyncio.run(rebuild_indexes())

The return value is the number of queue records examined. The rebuild is
explicit, unbounded, and idempotent, so an interrupted call is safe to repeat.
Restart the writers only after it completes.

Worker wakeups
==============

``worker_wakeups=True`` publishes non-durable worker hints. Workers still poll
the stored queue state. ``wakeup_channel`` and queue key prefixes belong
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

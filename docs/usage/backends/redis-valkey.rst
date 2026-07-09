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

Payloads and key isolation
==========================

Task arguments, keyword arguments, metadata, results, and errors must be JSON
serializable. Give each application and environment a distinct ``key_prefix``;
do not use ``FLUSHALL`` for test cleanup on shared infrastructure.

Worker wakeups
==============

``notifications=True`` publishes non-durable worker hints. Workers still poll
the stored queue state. ``notification_channel`` and queue key prefixes belong
to queue operations, not browser Channels.

Event history and live delivery
===============================

Backend-managed event history is supported. Redis/Valkey operators control
persistence configuration, retention, backups, memory policy, and cleanup;
the library cannot make an ephemeral service durable.

A Redis or Valkey queue backend does not automatically provide live fan-out.
Configure a shared Channels backend with a separate Channels key prefix for
standalone workers and multiple web processes. Redis pub/sub is ephemeral
broadcast; Redis Streams can retain backlog. Keep stream authorization at the
Litestar route boundary.

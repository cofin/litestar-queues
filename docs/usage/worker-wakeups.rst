==============
Worker wakeups
==============

When no work is available, a worker asks the queue backend to wait for a
notification with ``WorkerConfig.poll_interval`` as the timeout:

.. code-block:: python

   from litestar_queues import QueueConfig, WorkerConfig

   queue_config = QueueConfig(worker=WorkerConfig(poll_interval=0.25))

The effective wait ends when a backend notification arrives, the timeout
expires, or worker shutdown is requested. Backends without notification
support simply implement the timeout as polling.

Hints, not state
================

Redis and Valkey use pub/sub hints. SQLSpec turns on its best native wakeup
transport automatically for any capable adapter (PostgreSQL ``LISTEN``/``NOTIFY``
for asyncpg, psycopg, and psqlpy; a durable in-process queue for DuckDB) and
falls back to polling elsewhere. Advanced Alchemy can use PostgreSQL
notification hints when enabled. The message only tells the worker to check. The
saved task record has the real state.

Notifications may arrive late or not at all. The worker stays correct by
checking the saved task state. Do not set the poll interval higher than the
delay your service can tolerate.

Separate from task events
=========================

Worker wakeups are not ``QueueEvent`` delivery, browser fan-out, or durable
event history. A Redis queue backend does not automatically configure Redis
Channels for SSE or WebSocket consumers. See :doc:`events`.

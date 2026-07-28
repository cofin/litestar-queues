==============
Worker wakeups
==============

When no work is available, a worker asks the queue backend to wait for a
notification. The timeout begins at ``WorkerConfig.poll_interval``:

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

Notifications may arrive late, be coalesced, or not arrive at all. Every worker
cycle checks durable queue state before it waits, so a later polling pass still
claims the task. The same reconciliation also discovers due scheduled and retry
records; those known due times can shorten the next wait.

Native listener lifetime
========================

A normal wait timeout does not tear down and recreate a native listener. Memory,
Redis, Valkey, SQLSpec, and Advanced Alchemy retain at most one in-flight native
read and resume it on the next wait. Shutdown cancels that read. A listener read
failure is logged, resets adaptive backoff, and lets the next wait establish a
clean listener while durable polling continues to provide correctness.

Adaptive polling
================

After a fully empty cycle, the worker multiplies its stored interval by
``poll_backoff_multiplier`` until ``poll_backoff_max``. Jitter changes only the
sampled wait and remains clamped between the base and maximum; it never changes
the stored exponential state. Claimed work, a native notification, worker
startup, or a recoverable listener/backend error resets the interval
immediately.

The defaults are ``poll_interval=0.1``, ``poll_backoff_max=30.0``,
``poll_backoff_multiplier=2.0``, and ``poll_jitter=0.15``. Configuration
requires a positive base interval, a maximum at least as large as the base, a
multiplier of at least ``1.0``, and jitter from ``0.0`` through ``1.0``. Set
``poll_backoff_max=None`` for fixed-interval polling.

For a polling-only backend with no already-known due time,
``poll_backoff_max`` is the worst-case discovery delay. Native hints can end
the wait sooner. Choose a maximum no higher than the delay the service can
tolerate.

Separate from task events
=========================

Worker wakeups are not ``QueueEvent`` delivery, browser fan-out, or durable
event history. A Redis queue backend does not automatically configure Redis
Channels for SSE or WebSocket consumers. See :doc:`events`.

Transport counters and timings use the bounded label contract documented in
:doc:`observability`; backend pages link to that single canonical metric
reference rather than redefining it.

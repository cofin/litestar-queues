==============
Worker wakeups
==============

When no work is available, a worker asks the queue backend to wait for a
notification with ``worker_poll_interval`` as the timeout:

.. code-block:: python

   from litestar_queues import QueueConfig

   queue_config = QueueConfig(worker_poll_interval=0.25)

The effective wait ends when a backend notification arrives, the timeout
expires, or worker shutdown is requested. Backends without notification
support simply implement the timeout as polling.

Hints, not state
================

Redis and Valkey use pub/sub hints. SQLSpec can use a configured SQLSpec event
transport. Advanced Alchemy can use PostgreSQL notification hints when enabled.
These messages say “check the queue”; they do not contain the authoritative
task lifecycle.

Notifications can be coalesced, delayed, or dropped. Correctness comes from
polling and reconciliation against the durable task table or data structure.
Do not increase the poll interval beyond the recovery latency your service can
tolerate.

Separate from task events
=========================

Worker wakeups are not ``QueueEvent`` delivery, browser fan-out, or durable
event history. A Redis queue backend does not automatically configure Redis
Channels for SSE or WebSocket consumers. See :doc:`events`.

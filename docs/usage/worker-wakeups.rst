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
The message only tells the worker to check. The saved task record has the real
state.

Notifications may arrive late or not at all. The worker stays correct by
checking the saved task state. Do not set the poll interval higher than the
delay your service can tolerate.

Separate from task events
=========================

Worker wakeups are not ``QueueEvent`` delivery, browser fan-out, or durable
event history. A Redis queue backend does not automatically configure Redis
Channels for SSE or WebSocket consumers. See :doc:`events`.

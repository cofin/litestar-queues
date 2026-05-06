============
Introduction
============

Litestar Queues is built around three independent runtime boundaries:

* ``QueueBackend`` persists queue records and task lifecycle state.
* ``ExecutionBackend`` decides where a claimed record runs.
* ``QueueEventSink`` delivers lifecycle, progress, log, and custom task events
  to application-owned realtime infrastructure.

The core install includes an in-memory queue backend plus immediate and local
execution backends. SQLSpec, Advanced Alchemy, Redis, Valkey, and Cloud Run are
optional integrations that are imported only when their backend is opened or
when the application injects an already configured client.

Task Lifecycle
==============

Tasks are registered with the :func:`litestar_queues.task` decorator. Enqueueing
a task creates a queue record containing the task name, arguments, keyword
arguments, queue name, priority, retry policy, scheduled time, execution backend,
execution profile, deduplication key, and metadata.

Workers claim due records, execute the registered callable, update the persisted
record, retry eligible failures, and publish lifecycle events. The same task can
use different execution backends per enqueue call, so local development,
database-backed workers, and external execution can share the same task code.

Realtime Events
===============

Queue events are separate from backend wakeup notifications. Wakeup mechanisms,
such as Redis pub/sub or SQLSpec Events, are non-durable worker hints. Queue
events are application-facing envelopes for lifecycle, progress, logs, and
custom task state that can be sent to Litestar Channels, an in-memory test sink,
or a custom sink.

Use Cases
=========

Litestar Queues fits applications that need:

* background jobs started from HTTP routes,
* scheduled recurring work,
* status and result polling,
* progress updates streamed to clients,
* durable queue records in SQL, Redis, or Valkey,
* external task execution in Cloud Run Jobs.

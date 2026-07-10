===========
Task events
===========

Task events tell applications and operators about a task's lifecycle, progress,
logs, or custom state. They are not the queue-backend notifications that wake
workers. Delivering an event does not help a worker find or claim a task, and
it does not store the queue record.

Enable publishing
=================

Provide a sink or Channels backend:

.. code-block:: python

   from litestar_queues import QueueConfig
   from litestar_queues.events import EventConfig

   queue_config = QueueConfig(
       event=EventConfig(
           channels_backend=channels_backend,
           publish_global_lifecycle=True,
       )
   )

Without a configured sink or Channels backend, publishing does nothing. By
default, a live-delivery failure does not fail the task. Set ``strict=True``
only when the caller must receive a sink error.

Publish from a task
===================

.. code-block:: python

   from litestar_queues import task
   from litestar_queues.events import publish_task_log, publish_task_progress


   @task("imports.process", timeout=300)
   async def process_import(path: str) -> None:
       await publish_task_log("Import started", payload={"path": path})
       await publish_task_progress(current=50, total=100, message="Halfway")

The active task context adds the task ID, task name, queue, worker ID, attempt,
execution backend, and sequence. Use ``publish_task_event()`` for a custom
event type. Accept ``_task_context`` when you prefer to call the context
methods directly.

Buffering and external producers
================================

When enabled, history is written before live delivery. Non-terminal live events
are sent in small batches, and the buffer is flushed before the final event.
Sinks with ``publish_many`` receive a batch; other sinks receive the events one
at a time in order.

Code outside a worker should use this context manager:

.. code-block:: python

   from litestar_queues.events import create_event_producer

   async with create_event_producer(queue_config) as events:
       await events.task(task_id).progress(current=1, total=2, message="Started")

The context manager opens the resource, starts it, flushes pending events, and
closes it. ``QueueEventProducer`` does not manage resources by itself.

Topology and security
=====================

.. list-table::
   :header-rows: 1

   * - Example topology
     - Live delivery
     - Boundary
   * - Memory WebSocket/SSE
     - Same-process ``MemoryChannelsBackend``
     - Local demo; web and worker stay together.
   * - Separate Redis/Valkey worker
     - Explicit shared Channels backend
     - Use separate queue/Channels prefixes and authenticated services.
   * - Multiple web replicas
     - Broadcast-capable shared Channels transport
     - Authorize task/queue/worker/custom scope subscriptions.

SQLSpec durable table queues are shared work queues: one consumer claims each
record. They are not broadcast delivery for multiple browser-serving processes.

Next steps
==========

* :doc:`event-streams` exposes SSE and WebSocket endpoints.
* :doc:`event-history` retains backend-managed history.
* :doc:`event-testing` tests delivery without external infrastructure.
* :doc:`../examples/index` runs the canonical visual examples.

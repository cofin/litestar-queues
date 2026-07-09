===========
Task events
===========

Task events are application-facing lifecycle, progress, log, and custom
messages. They are distinct from queue-backend notifications that wake
workers. Event delivery does not discover work, claim records, or replace
queue persistence.

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

Without a configured sink or Channels backend, event publishing is a no-op.
Live delivery is best effort by default; set ``strict=True`` only when a sink
failure should fail the publishing path.

Publish from a task
===================

.. code-block:: python

   from litestar_queues import task
   from litestar_queues.events import publish_task_log, publish_task_progress


   @task("imports.process", timeout=300)
   async def process_import(path: str) -> None:
       await publish_task_log("Import started", payload={"path": path})
       await publish_task_progress(current=50, total=100, message="Halfway")

The active task context fills in task ID, task name, queue, worker identity,
attempt, execution backend, and sequence. Use ``publish_task_event()`` for a
custom type, or accept ``_task_context`` when direct context methods are more
convenient.

Buffering and external producers
================================

The publisher records configured history before live delivery. It micro-batches
non-terminal live events and flushes a task's buffered events before its
terminal event. Sinks with ``publish_many`` receive a batch; other sinks fall
back to ordered single-event ``publish`` calls.

Code outside a worker uses the lifecycle-owning context manager:

.. code-block:: python

   from litestar_queues.events import create_event_producer

   async with create_event_producer(queue_config) as events:
       await events.task(task_id).progress(current=1, total=2, message="Started")

The context manager opens the configured resource, starts and flushes the
buffer, then closes the resource. ``QueueEventProducer`` itself is a thin,
lifecycle-free facade.

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

SQLSpec durable table queues use competing-consumer semantics. They are not
broadcast fan-out for multiple browser-serving processes.

Next steps
==========

* :doc:`event-streams` exposes SSE and WebSocket endpoints.
* :doc:`event-history` retains backend-managed history.
* :doc:`event-testing` tests delivery without external infrastructure.
* :doc:`../examples/index` runs the canonical visual examples.

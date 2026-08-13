===========
Task events
===========

Task events tell applications and operators about a task's lifecycle, progress,
logs, or custom state. They are not the queue-backend notifications that wake
workers. Delivering an event does not help a worker find or claim a task, and
it does not store the queue record.

Enable publishing
=================

Provide Channels or one or more additive sinks:

.. code-block:: python

   from litestar_queues import QueueConfig
   from litestar_queues.events import EventDeliveryConfig, QueueEventsConfig

   queue_config = QueueConfig(
       events=QueueEventsConfig(
           channels=channels_backend,
           delivery=EventDeliveryConfig(publish_global_lifecycle=True),
       ),
   )

Without a configured sink or Channels backend, publishing does nothing. By
default, a live-delivery failure does not fail the task. Set ``strict=True``
only when the caller must receive a sink error.

Publish from a task
===================

Choose the signal that matches what happened:

.. list-table::
   :header-rows: 1

   * - Signal
     - Meaning
     - Owner
   * - Heartbeat
     - Liveness timestamp used for recovery; ``beat(detail)`` may attach the
       latest short diagnostic detail.
     - The worker updates timestamps automatically while a task runs.
   * - Progress
     - Typed, measurable movement such as ``current`` out of ``total``.
     - Task code publishes it when meaningful work advances.
   * - Custom event
     - A distinct domain occurrence, such as ``crawl.page_discovered``.
     - Task code publishes it when that occurrence happens.
   * - Lifecycle event
     - A queue-owned state transition such as ``task.started``,
       ``task.completed``, or ``task.failed``.
     - The service and worker publish it automatically.

Tasks do not need to call ``beat()`` to remain alive, and a progress update
should not also be copied into a generic custom event. Use ``beat(detail)``
only when a short, last-value diagnostic helps recovery or operations.

The context methods make those boundaries explicit:

.. code-block:: python

   from litestar_queues import task
   from litestar_queues.events import TaskExecutionContext


   @task("crawl.run", timeout=300)
   async def crawl(*, _task_context: TaskExecutionContext) -> dict[str, int]:
       ctx = _task_context

       # Standardized progress state for status pages and UI consumers.
       await ctx.progress(
           current=3,
           total=6,
           message="3/6 pages",
           payload={"page": 3},
       )

       # An application occurrence; it does not update progress or terminal
       # task state. False keeps ordinary events eligible for buffering.
       await ctx.event(
           "crawl.page_discovered",
           message="Discovered the queue guide",
           payload={"url": "https://example.invalid/queues"},
           immediate=False,
       )

       # Optional last-value diagnostic for the next automatic heartbeat.
       ctx.beat("Parsing the final page")
       return {"pages": 6}

``ctx.progress(current=..., total=..., message=..., payload=...)`` publishes
``task.progress`` and derives a percentage when ``current`` and ``total`` are
available. ``ctx.event(name, message=..., payload=..., immediate=False)``
publishes the supplied application event name. A custom event does not update
progress or terminal task state. Set ``immediate=True`` only when that event
must bypass the ordinary live-event buffer.

Automatic worker heartbeats keep active jobs live. ``ctx.beat(detail)`` is an
optional, last-value-wins detail update for the next heartbeat write; it is not
a liveness requirement and does not publish a task event by itself.

The active task context adds the task ID, task name, queue, worker ID, attempt,
execution backend, and sequence.

A running task never has to bind that context. The service binds it before it
calls the task body, so the module-level helpers resolve it on their own:

.. code-block:: python

   from litestar_queues import task
   from litestar_queues.events import publish_task_progress


   @task("catalog.import")
   async def import_catalog() -> None:
       await publish_task_progress(current=13, total=400)

Each helper is a pass-through to the context method of the same name, so
``publish_task_progress(...)`` and ``ctx.progress(...)`` do the same work.
Prefer the context method in a task body, where the context is already in hand.
Reach for a helper in a function further down the call stack, so it can report
progress without threading ``ctx`` through every signature in between. A helper
raises :exc:`RuntimeError` when no context is bound.

Context injection is keyed on the parameter **name** ``_task_context``, not on
its type annotation: a parameter annotated ``TaskExecutionContext`` under any
other name receives nothing. A task declaring ``**kwargs`` also receives the
context under that key.

Keep payloads small and JSON-serializable. Put large files, crawled documents,
and model artifacts in external storage and send a stable reference in the
payload.

Buffering and external producers
================================

When enabled, history is written before live delivery. Non-terminal live events
are sent in small batches, and the buffer is flushed before the final event.
Sinks with ``publish_many`` receive a batch; other sinks receive the events one
at a time in order.

Retries may produce another ``task.started`` for the same task ID.
``task.failed`` includes ``will_retry`` so a consumer can distinguish an
attempt failure from a terminal failure. Cancellation, claim loss, and stale
failure are separate terminal paths; consumers should not infer persisted
result data from the event payload alone. After any terminal event, refresh the
:class:`~litestar_queues.TaskResult` when the record, result, or error is
needed.

Returning normally stores the return value and publishes ``task.completed``.
Raising follows the task's retry policy: ``task.failed`` reports
``will_retry=true`` when the record was requeued, and a later attempt publishes
another ``task.started``. With no retry remaining, the record and event are
terminally failed. Task code should not publish its own completed or failed
lifecycle event.

Code outside a worker should use this context manager:

.. code-block:: python

   from litestar_queues.events import create_event_producer

   async with create_event_producer(queue_config) as events:
       await events.task(task_id).progress(current=1, total=2, message="Started")

The context manager opens the resource, starts it, flushes pending events, and
closes it. ``QueueEventProducer`` does not manage resources by itself.

Using events without the queue
==============================

The ``litestar_queues.events`` subpackage also runs on its own, for a runtime
that has its own task runner and never starts this package's worker. That is a
separate integration path with its own setup — see
:doc:`events-standalone`. Nothing on this page requires it.

For durable, queryable history — including extra scoping dimensions such as a
tenant or project id — see :doc:`event-history`.

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
* :doc:`event-history` retains backend-managed history and adds extra scoping
  dimensions such as a tenant id.
* :doc:`event-testing` tests delivery without external infrastructure.
* :doc:`../examples/index` runs the canonical visual examples.

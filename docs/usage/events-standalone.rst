==========================
Events without the queue
==========================

.. note::

   This page is only for runtimes that never run this package's worker. If you
   enqueue work with ``@task`` and a queue backend, none of this applies:
   publishing is the one-line call in :doc:`events`, and the service binds a
   context for you before it calls your task body. Constructing a
   ``TaskExecutionContext`` by hand there would shadow the real one.

``litestar_queues.events`` works on its own. It needs only ``litestar`` and
``typing_extensions``: no queue backend, no worker, and no queue record. A
runtime that already has its own task runner can bind a task context and use
the same event surface the package's own worker uses, rather than reimplementing
the envelope, the sinks, and the chunking.

Binding a task context
======================

``bind_task_context()`` binds a :class:`~litestar_queues.events.TaskExecutionContext`
for the duration of a ``with`` block. While it is bound,
``get_current_task_context()`` and the module-level publish helpers resolve to
it. Setting a context variable is synchronous, so one plain ``with`` block works
inside both sync and async task bodies:

.. code-block:: python

   import asyncio

   from litestar_queues.events import (
       InMemoryQueueEventSink,
       QueueEventPublisher,
       TaskExecutionContext,
       bind_task_context,
   )


   async def main() -> None:
       sink = InMemoryQueueEventSink()
       publisher = QueueEventPublisher(sink)
       context = TaskExecutionContext(
           task_id="import-42",
           task_name="catalog.import",
           queue="default",
           worker_id="runner-1",
           execution_backend="external",
           execution_profile=None,
           attempt=1,
           event_publisher=publisher,
       )

       with bind_task_context(context) as ctx:
           await ctx.progress(current=12, total=400, message="loading")

       print([event.type for event in sink.events])


   asyncio.run(main())

Receiving beats
===============

``bind_beat_sink()`` binds the receiver for ``ctx.beat(detail)``, the optional
last-value-wins diagnostic detail. Implement
:class:`~litestar_queues.events.TaskBeatSink` to receive it:

.. code-block:: python

   from litestar_queues.events import TaskBeatSink, bind_beat_sink


   class LastBeat(TaskBeatSink):
       def __init__(self) -> None:
           self.detail: str | None = None

       def record_beat(self, task_id: str, detail: str | None) -> None:
           self.detail = detail


   beats = LastBeat()
   with bind_task_context(context), bind_beat_sink(beats):
       context.beat("row 30000")

Cancellation
============

Durable cancellation fan-in stays internal: the package's own worker binds it
when it owns the queue record. An external runtime cancels through its own
mechanism and can still surface that to task code by calling
``context.mark_cancelled()``.

For durable, queryable history — including extra scoping dimensions such as a
tenant or project id — see :doc:`event-history`.

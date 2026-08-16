=============
Event history
=============

Event history saves task events in the queue backend so you can query them
later and review the stages a task moved through. For how history differs from
a live SSE or WebSocket stream, see :ref:`live-delivery-vs-history`.

Enable history
==============

.. code-block:: python

   from litestar_queues import QueueConfig
   from litestar_queues.events import EventHistoryConfig, QueueEventsConfig

   queue_config = QueueConfig(
       events=QueueEventsConfig(
           history=EventHistoryConfig(
               batch_size=20,
               flush_interval=1.0,
               memory_capacity=1000,
           ),
       ),
   )

History is independent of live delivery: configure ``delivery`` and ``channels``
only when you also want a live stream, as shown in :doc:`events`.

With ``strict=False``, a history write failure does not fail the task. Use
``strict=True`` only when saving every event is required.

Support matrix
==============

.. list-table::
   :header-rows: 1

   * - Backend
     - Support and persistence boundary
   * - Memory
     - Bounded, temporary history. ``EventHistoryConfig.memory_capacity`` sets the limit in that process.
   * - SQLSpec
     - History stored in the SQLSpec queue schema.
   * - Advanced Alchemy
     - History stored through an app-owned event-log model and migrations.
   * - Redis / Valkey
     - Shared history. You choose how long it stays and whether it is backed up.

Query and cleanup
=================

``QueueService.get_event_log()`` returns the backend's
:class:`~litestar_queues.events.QueueEventLog` when history is enabled, and
``None`` when it is not. Use it to find events by task ID, task name, or actor,
review stages, flush pending writes, and delete old records. Choose retention
rules that fit your audit and privacy needs. Deleting finished task records does
not delete event history, and vice versa.

Filtering by actor
==================

An event may carry a ``QueueEventActor`` naming who or what caused it, an ``entity`` string
naming the record it acted on, and a ``scope`` / ``scope_key`` pair to group related work.
History stores these dimensions and lets you filter on them. Attach them by publishing a
:class:`~litestar_queues.events.QueueEvent` you build yourself:

.. code-block:: python

   import asyncio

   from litestar_queues import QueueConfig, QueueService, WorkerConfig
   from litestar_queues.events import (
       EventHistoryConfig,
       QueueEvent,
       QueueEventActor,
       QueueEventQuery,
       QueueEventsConfig,
   )


   async def main() -> None:
       config = QueueConfig(
           queue_backend="memory",
           execution_backend="immediate",
           worker=WorkerConfig(placement="external"),
           events=QueueEventsConfig(history=EventHistoryConfig()),
       )
       async with QueueService(config) as service:
           publisher = service.get_event_publisher()
           event_log = service.get_event_log()
           if event_log is None:
               msg = "Enable EventHistoryConfig to record and query history."
               raise RuntimeError(msg)

           await publisher.publish(
               QueueEvent(
                   type="task.log",
                   scope="import_batch",
                   scope_key="batch-42",
                   entity="user-csv",
                   stage="processing",
                   task_id="import-42",
                   message="importing",
                   actor=QueueEventActor(type="user", id="u-1", name="Alice"),
               )
           )
           await event_log.flush_events()

           # Query events by any dimension using QueueEventQuery
           page = await event_log.query_events(QueueEventQuery(actor_id="u-1", scope_key="batch-42"))
           print(len(page.items))


   asyncio.run(main())

Filters use equality and are ANDed together. Every backend stores these fields and
answers the query; the SQLSpec and Advanced Alchemy tables index
``(actor_id, occurred_at)``, ``(scope_key, occurred_at)``, and ``(entity, occurred_at)``
to match the time-ordered read pattern.

The actor's ``name`` is not stored. It is mutable display text that would go
stale against the event it was stamped on, so it travels on the live event
envelope only. Resolve names from your own user or service directory when you
render history.

Scheduling cleanup
==================

Configure a bounded event-history phase and run it from one external schedule:

.. code-block:: python

   from litestar_queues import QueueConfig, QueueMaintenanceConfig
   from litestar_queues.maintenance import QueueEventRetentionRule
   from litestar_queues.events import EventHistoryConfig, QueueEventQuery, QueueEventsConfig

   queue_config = QueueConfig(
       queue_backend="redis",
       events=QueueEventsConfig(history=EventHistoryConfig()),
       maintenance=QueueMaintenanceConfig(
           event_retention_rules=(
               # Retain everything for 30 days
               QueueEventRetentionRule(max_age=30 * 24 * 60 * 60),
               # Retain debug events for only 7 days
               QueueEventRetentionRule(
                   match=QueueEventQuery(level="debug"),
                   max_age=7 * 24 * 60 * 60,
               ),
           ),
           event_limit=1000,
       ),
   )

Then schedule ``litestar queues run-maintenance``. It deletes at most
``event_limit`` oldest matching rows in one invocation. The rules are evaluated
so that the longest retention period matching an event applies. You can also use ``exclude``
to build negative matches (e.g. retaining everything except "task.started").

Terminal-task retention is a separate setting, so the two policies can use different cutoffs. See
:doc:`maintenance` for coordination, cadence, backend, and migration requirements.

Memory history is bounded by ``memory_capacity`` and disappears with the process.
SQLSpec, Advanced Alchemy, Redis, and Valkey history is durable or shared, so
those deployments should include cleanup in their backup and privacy policies.

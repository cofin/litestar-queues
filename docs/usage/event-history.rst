=============
Event history
=============

Event history saves task events in the queue backend. You can query the records
later and review their stages. It is separate from live SSE/WebSocket delivery.

Enable history
==============

.. code-block:: python

   from litestar_queues import QueueConfig
   from litestar_queues.events import EventConfig, EventLogConfig

   queue_config = QueueConfig(
       event=EventConfig(channels_backend=channels_backend),
       event_log=EventLogConfig(
           buffer_size=20,
           flush_interval=1.0,
           max_records=1000,
       ),
   )

With ``strict=False``, a history write failure does not fail the task. Use
``strict=True`` only when saving every event is required.

Support matrix
==============

.. list-table::
   :header-rows: 1

   * - Backend
     - Support and persistence boundary
   * - Memory
     - Bounded, temporary history. ``EventLogConfig.max_records`` sets the limit in that process.
   * - SQLSpec
     - History stored in the SQLSpec queue schema.
   * - Advanced Alchemy
     - History stored through an app-owned event-log model and migrations.
   * - Redis / Valkey
     - Shared history. You choose how long it stays and whether it is backed up.

Query and cleanup
=================

Use ``QueueEventLog`` to find events by task ID or task name, review stages,
flush pending writes, and delete old records. Choose retention rules that fit
your audit and privacy needs. Deleting finished task records does not delete
event history, and vice versa.

Run cleanup from an application-owned scheduler, cron job, or maintenance
worker. The library does not create a hidden cleanup task, because retention
and the number of maintenance workers are deployment decisions:

.. code-block:: python

   from datetime import datetime, timedelta, timezone

   from litestar_queues import QueueService
   from litestar_queues.events import EventLogConfig

   async def prune_queue_history(service: QueueService) -> None:
       cutoff = datetime.now(timezone.utc) - timedelta(days=30)
       backend = service.get_queue_backend()

       # These are separate policies. Use different cutoffs when needed.
       await backend.cleanup_terminal(cutoff)
       event_log = backend.get_event_log(EventLogConfig(enabled=True))
       if event_log is not None:
           await event_log.flush_events()
           await event_log.cleanup_before(cutoff)

Memory history is bounded by ``max_records`` and disappears with the process.
SQLSpec, Advanced Alchemy, Redis, and Valkey history is durable or shared, so
those deployments should schedule cleanup and include it in their backup and
privacy policies.

History versus live delivery
============================

History answers “what happened?” after the fact. SSE/WebSocket Channels answer
“what is happening now?” A deployment may use either or both. Replaying
history into a newly connected client is an application policy; do not assume
a live Channels backend reads the queue event log.

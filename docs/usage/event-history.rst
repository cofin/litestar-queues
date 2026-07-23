=============
Event history
=============

Event history saves task events in the queue backend. You can query the records
later and review their stages. It is separate from live SSE/WebSocket delivery.

Enable history
==============

.. code-block:: python

   from litestar_queues import QueueConfig
   from litestar_queues.events import EventDeliveryConfig, EventHistoryConfig, QueueEventsConfig

   queue_config = QueueConfig(
       events=QueueEventsConfig(
           channels=channels_backend,
           delivery=EventDeliveryConfig(),
           history=EventHistoryConfig(
               batch_size=20,
               flush_interval=1.0,
               memory_capacity=1000,
           ),
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
     - Bounded, temporary history. ``EventHistoryConfig.memory_capacity`` sets the limit in that process.
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

Configure a bounded event-history phase and run it from one external schedule:

.. code-block:: python

   from litestar_queues import QueueConfig, QueueMaintenanceConfig
   from litestar_queues.events import EventHistoryConfig, QueueEventsConfig

   queue_config = QueueConfig(
       queue_backend=...,
       events=QueueEventsConfig(history=EventHistoryConfig()),
       maintenance=QueueMaintenanceConfig(
           event_retention=30 * 24 * 60 * 60,
           event_limit=1000,
       ),
   )

Then schedule ``litestar queues run-maintenance``. It deletes at most
``event_limit`` oldest matching rows in one invocation. Terminal-task retention
is a separate setting, so the two policies can use different cutoffs. See
:doc:`maintenance` for coordination, cadence, backend, and migration requirements.

Memory history is bounded by ``memory_capacity`` and disappears with the process.
SQLSpec, Advanced Alchemy, Redis, and Valkey history is durable or shared, so
those deployments should include cleanup in their backup and privacy policies.

History versus live delivery
============================

History answers “what happened?” after the fact. SSE/WebSocket Channels answer
“what is happening now?” A deployment may use either or both. Replaying
history into a newly connected client is an application policy; do not assume
a live Channels backend reads the queue event log.

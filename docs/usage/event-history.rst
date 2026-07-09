=============
Event history
=============

Event history records task events through the queue backend before live sink
delivery. It supports later queries and stage summaries; it is not a live sink
and does not fan events out to browsers.

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

``strict=False`` keeps history failures from turning successful task work into
failure. Choose ``strict=True`` only when event retention is a business
transaction requirement.

Support matrix
==============

.. list-table::
   :header-rows: 1

   * - Backend
     - Support and persistence boundary
   * - Memory
     - Bounded ephemeral history. ``EventLogConfig.max_records`` caps retained records in the process.
   * - SQLSpec
     - Durable history in the SQLSpec queue schema and migration/session lifecycle.
   * - Advanced Alchemy
     - Durable history through an application-owned concrete event-log model and migrations.
   * - Redis / Valkey
     - Shared history whose durability and cleanup depend on operator persistence, retention, and backup policy.

Query and cleanup
=================

The backend-owned ``QueueEventLog`` lists events by task ID or task name,
summarizes stages, flushes buffered writes, and removes records older than a
timestamp. Retention should match audit and privacy requirements. Terminal
task-record cleanup and event-history cleanup are separate operations.

History versus live delivery
============================

History answers “what happened?” after the fact. SSE/WebSocket Channels answer
“what is happening now?” A deployment may use either or both. Replaying
history into a newly connected client is an application policy; do not assume
a live Channels backend reads the queue event log.

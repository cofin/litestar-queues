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

SQLSpec, Advanced Alchemy, Redis and Valkey buffer history until ``batch_size``
is reached or the oldest pending batch reaches ``flush_interval``. The timer
runs even when no more events arrive. This interval schedules a write; event-loop
delays and storage failures can postpone commitment.

``max_pending`` defaults to 2000 and must be at least ``batch_size``. It bounds
accepted records awaiting persistence or live release. Admission raises
``QueueEventBufferFull`` when full, regardless of ``strict``. Configuration
errors also propagate. SQLSpec, Advanced Alchemy and Redis/Valkey reject
conflicting reuse of an event ID.

With ``strict=False``, transient write failures retain accepted records for a
bounded retry and withhold their live delivery. With ``strict=True``, foreground
write failures propagate; a timer failure is surfaced at the next foreground
boundary. Neither setting makes uncommitted history safe from process death.
An orderly close attempts the remaining writes; non-strict close logs unresolved
records instead of delivering them live.

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

An event may carry a ``QueueEventActor`` naming who or what caused it, a typed
``QueueEventEntityRef`` naming the affected record, and a ``scope`` /
``scope_key`` pair grouping related work. Put a stage in the event's ``payload``.
History stores the entity as ``type:id``. Actor filters use the existing
``extra={"actor_id": ...}`` or ``extra={"actor_type": ...}`` argument;
``QueueEventQuery`` has no typed actor field.

Run this complete example with ``uv run python examples/event_history.py``.
It prints ``1`` and checks the stored stage and entity:

.. literalinclude:: ../../examples/event_history.py
   :language: python

Filters use equality and are ANDed together. Every backend stores these fields and
answers the query; the SQLSpec and Advanced Alchemy tables index
``(actor_id, occurred_at)``, ``(scope_key, occurred_at)``, and ``(entity, occurred_at)``
to match the time-ordered read pattern.

The actor's ``name`` is not stored. It is mutable display text that would go
stale against the event it was stamped on, so it travels on the live event
envelope only. Resolve names from your own user or service directory when you
render history.

``page.total`` counts all matching rows before pagination, even when an offset
produces an empty page. SQLSpec selects the page and count in the same session;
consistency during concurrent writes follows the configured transaction
isolation, without an additional snapshot guarantee.

History ownership and custom providers
======================================

``QueueService`` closes its history before the live buffer and backend resources.
Direct event-log callers own ``await event_log.aclose()``. For buffered SQLSpec,
Advanced Alchemy and Redis/Valkey logs, closing stops admission; an explicit
backend or service reopen creates a fresh coordinator. Memory and Ephemeral
history persist immediately within their storage boundary and have no history
timer to close. Flush and close attempt buffered history before releasing its
live callbacks. A live sink failure never puts already committed rows back into
the history buffer.

Custom ``QueueEventLog`` providers must implement
``publish_event_after_commit(event, *, release, barrier=False)`` and ``aclose()``.
Invoke ``release`` only after the history transaction has committed, including
successful exit from any transaction context manager. Missing methods are
rejected when attaching a provider. Persistence strictness belongs to
``EventHistoryConfig``; the publisher's former ``event_log_strict`` argument and
``set_event_log(strict=...)`` option have been removed.

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
               # Retain debug events for only 7 days
               QueueEventRetentionRule(
                   match=QueueEventQuery(level="debug"),
                   max_age=7 * 24 * 60 * 60,
               ),
               # Retain other events for 30 days
               QueueEventRetentionRule(max_age=30 * 24 * 60 * 60),
           ),
           event_limit=1000,
       ),
   )

Then schedule ``litestar queues run-maintenance``. It deletes at most
``event_limit`` oldest matching rows in one invocation. The rules are evaluated
in declaration order: the first matching rule owns the event. Put specific rules
before a catch-all rule. You can also use ``exclude``
to build negative matches (e.g. retaining everything except "task.started").

Terminal-task retention is a separate setting, so the two policies can use different cutoffs. See
:doc:`maintenance` for coordination, cadence, backend, and migration requirements.

Memory history is bounded by ``memory_capacity`` and disappears with the process.
SQLSpec, Advanced Alchemy, Redis, and Valkey history is durable or shared, so
those deployments should include cleanup in their backup and privacy policies.

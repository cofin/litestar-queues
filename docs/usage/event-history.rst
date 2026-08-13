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

Extra scoping dimensions
========================

Some deployments scope events by a dimension the queue does not model — a
tenant, project, or account. There are two supported ways to handle that.

Implement ``QueueEventLog`` yourself
------------------------------------

This is the supported path for arbitrary dimensions, custom indexes, or scoped
aggregates. ``QueueEventLog`` is a ``Protocol``, so any object with the right
methods satisfies it — no subclassing and no package change:

.. code-block:: python

   from datetime import datetime

   from litestar_queues.events import QueueEvent, QueueEventLogRecord, QueueEventStageSummary


   class TenantEventLog:
       def __init__(self, tenant_id: str) -> None:
           self.tenant_id = tenant_id

       async def publish_event(self, event: QueueEvent) -> None:
           ...  # write the event with your own tenant column and indexes

       async def flush_events(self) -> None:
           ...

       async def list_events(
           self, *, task_id=None, task_name=None, limit=None
       ) -> list[QueueEventLogRecord]:
           ...  # your own query surface, scoped however you need

       async def summarize_stages(self, *, task_name=None) -> list[QueueEventStageSummary]:
           ...

       async def cleanup_before(self, before: datetime, *, limit=None) -> int:
           ...

You own the storage, the schema, and the query API, and you can expose scoped
aggregates the built-in store does not.

Declaring extra columns on the built-in SQLSpec store
------------------------------------------------------

If you want to keep the built-in SQLSpec store and only need indexed, filterable
columns, declare them on the backend config:

.. code-block:: python

   from litestar_queues.backends.sqlspec import EventHistoryExtraColumn, SQLSpecBackendConfig

   backend_config = SQLSpecBackendConfig(
       sqlspec_config=sqlspec_config,
       event_history_extra_columns=(
           EventHistoryExtraColumn(name="tenant_id", source="tenant_id", indexed=True),
       ),
   )

Each declaration adds one column to the event-history table. ``source`` is the
key read from the event payload, so publishing carries the value automatically:

.. code-block:: python

   await publish_task_log("importing", payload={"tenant_id": "acme"})

Query it with the additive ``extra`` filter on the SQLSpec event log:

.. code-block:: python

   event_log = backend.get_event_log(history_config)
   records = await event_log.list_events(extra={"tenant_id": "acme"})

``extra`` uses equality and is ANDed with ``task_id`` and ``task_name``. An
undeclared key raises ``ValueError`` naming the declared columns, so the filter
never reaches SQL unvalidated.

A declared name must be a valid unquoted SQL identifier and must not collide
with a column the package already owns. The names ``scope``, ``scope_key``,
``actor``, and ``entity`` are also rejected: they are reserved for built-in
scoping dimensions. Every name check — package-owned columns, reserved names,
and duplicates between your own declarations — ignores case, because unquoted
SQL identifiers fold case, so ``TASK_ID`` and ``task_id`` are one column to the
database. A rejected declaration raises ``QueueConfigurationError`` when the
config is built, rather than failing later against the database.

Three things to know:

* Values are stored as text and read from the event payload. They stay in
  ``detail`` too — the column is an indexable, filterable copy, and ``detail``
  remains the complete record.
* The ``extra`` filter lives on the concrete SQLSpec event log, not on the
  ``QueueEventLog`` protocol. Code holding the protocol type sees the unchanged
  signature; code that filters holds the concrete store from
  ``SQLSpecQueueBackend.get_event_log()``.
* ``summarize_stages`` does not accept the filter. Scoped aggregates are a
  reason to implement the protocol instead.

Both the managed schema and the packaged migration emit the same DDL for
declared columns, so a migrated database and a backend-created one match.

Choosing between them
---------------------

Extra columns give you indexed, filterable, authorization-pushdown-capable
dimensions with no protocol churn — but only equality filtering, only text
values, and only on the concrete SQLSpec store. Implementing ``QueueEventLog``
costs more code and gives you everything: any number of dimensions, any types,
your own indexes, and your own query and aggregate surface. Start with the
columns if a tenant id and an equality filter is the whole requirement; move to
the protocol when it is not.

History versus live delivery
============================

History answers “what happened?” after the fact. SSE/WebSocket Channels answer
“what is happening now?” A deployment may use either or both. Replaying
history into a newly connected client is an application policy; do not assume
a live Channels backend reads the queue event log.

==============================
Extending event history
==============================

.. note::

   This page is only for deployments that scope events by a dimension the queue
   does not model — a tenant, project, or account. If you enable history and
   query it by task ID, task name, or actor, none of this applies: everything
   you need is in :doc:`event-history`.

There are two supported ways to add a scoping dimension. Declaring extra
columns keeps the built-in SQLSpec store and adds indexed, equality-filterable
text columns. Implementing :class:`~litestar_queues.events.QueueEventLog`
replaces the store entirely and gives you your own schema, indexes, and query
surface.

Declaring extra columns on the built-in SQLSpec store
=====================================================

If you want to keep the built-in SQLSpec store and only need indexed,
filterable columns, declare them on the backend config:

.. code-block:: python

   from litestar_queues.backends.sqlspec import EventHistoryExtraColumn, SQLSpecBackendConfig

   backend_config = SQLSpecBackendConfig(
       sqlspec_config=sqlspec_config,
       event_history_extra_columns=(
           EventHistoryExtraColumn(name="tenant_id", source="tenant_id", indexed=True),
       ),
   )

``sqlspec_config`` is the SQLSpec database config for your own database; see
:doc:`backends/sqlspec` for how to build one.

Each declaration adds one column to the event-history table. ``source`` is the
key read from the event payload, so publishing carries the value automatically
from inside a task body:

.. code-block:: python

   from litestar_queues import task
   from litestar_queues.events import publish_task_log


   @task("catalog.import")
   async def import_catalog(tenant_id: str) -> None:
       await publish_task_log("importing", payload={"tenant_id": tenant_id})

Query it with the additive ``extra`` filter on the SQLSpec event log. This
continues from the ``backend_config`` declared above:

.. code-block:: python

   from litestar_queues import QueueConfig, QueueService
   from litestar_queues.events import EventHistoryConfig, QueueEventsConfig


   async def tenant_history(tenant_id: str) -> None:
       queue_config = QueueConfig(
           queue_backend=backend_config,
           events=QueueEventsConfig(history=EventHistoryConfig()),
       )
       async with QueueService(queue_config) as service:
           event_log = service.get_event_log()
           if event_log is None:
               msg = "Enable EventHistoryConfig to query event history."
               raise RuntimeError(msg)
           records = await event_log.list_events(extra={"tenant_id": tenant_id})
           print(len(records))

``extra`` uses equality and is ANDed with the built-in ``task_id``,
``task_name``, ``actor_id``, and ``actor_type`` filters. An undeclared key
raises ``ValueError`` naming the declared columns, so the filter never reaches
SQL unvalidated.

A declared name must be a valid unquoted SQL identifier and must not collide
with a column the package already owns — including ``actor_type`` and
``actor_id``. The names ``scope``, ``scope_key``, and ``entity`` are also
rejected: they are reserved for built-in scoping dimensions. Every name check —
package-owned columns, reserved names, and duplicates between your own
declarations — ignores case, because unquoted SQL identifiers fold case, so
``TASK_ID`` and ``task_id`` are one column to the database. A rejected
declaration raises ``QueueConfigurationError`` when the config is built, rather
than failing later against the database.

Three things to know:

* Values are stored as text and read from the event payload. They stay in
  ``detail`` too — the column is an indexable, filterable copy, and ``detail``
  remains the complete record.
* The ``extra`` filter lives on the concrete SQLSpec event log, not on the
  :class:`~litestar_queues.events.QueueEventLog` protocol. ``get_event_log()``
  is annotated with the protocol, so a type checker needs a ``cast`` to the
  concrete SQLSpec store before it accepts the keyword.
* ``summarize_stages`` does not accept the filter. Scoped aggregates are a
  reason to implement the protocol instead.

Both the managed schema and the packaged migration emit the same DDL for
declared columns, so a migrated database and a backend-created one match.

Implementing ``QueueEventLog`` yourself
=======================================

This is the supported path for arbitrary dimensions, custom indexes, or scoped
aggregates. :class:`~litestar_queues.events.QueueEventLog` is a ``Protocol``, so
any object with the right methods satisfies it — no subclassing and no package
change.

The skeleton below is complete and runnable. It keeps records in a list so the
shape stays readable; replace that list with your own table, columns, and
queries.

.. code-block:: python

   from datetime import datetime, timezone

   from litestar_queues.events import QueueEvent, QueueEventLogRecord, QueueEventStageSummary


   class TenantEventLog:
       """Tenant-scoped event history."""

       def __init__(self, tenant_id: str) -> None:
           self.tenant_id = tenant_id
           self._records: list[QueueEventLogRecord] = []

       async def publish_event(self, event: QueueEvent) -> None:
           """Record one event, dropping anything outside this tenant."""
           detail = dict(event.payload)
           if detail.get("tenant_id") != self.tenant_id:
               return
           self._records.append(
               QueueEventLogRecord(
                   event_id=event.id,
                   event_type=event.type,
                   task_id=event.task_id,
                   task_name=event.task_name,
                   queue=event.queue,
                   worker_id=event.worker_id,
                   execution_backend=event.execution_backend,
                   execution_profile=event.execution_profile,
                   actor_type=event.actor.type if event.actor is not None else None,
                   actor_id=event.actor.id if event.actor is not None else None,
                   stage=detail.get("stage"),
                   level=event.level,
                   message=event.message,
                   detail=detail,
                   progress_current=event.progress_current,
                   progress_total=event.progress_total,
                   progress_percent=event.progress_percent,
                   duration_ms=detail.get("duration_ms"),
                   sequence=event.sequence,
                   occurred_at=event.occurred_at,
                   created_at=datetime.now(timezone.utc),
               )
           )

       async def flush_events(self) -> None:
           """Nothing is buffered here; write your pending batch to storage instead."""

       async def list_events(
           self,
           *,
           task_id: str | None = None,
           task_name: str | None = None,
           actor_id: str | None = None,
           actor_type: str | None = None,
           limit: int | None = None,
       ) -> list[QueueEventLogRecord]:
           """Return this tenant's newest matching records."""
           matches = [
               record
               for record in self._records
               if (task_id is None or record.task_id == task_id)
               and (task_name is None or record.task_name == task_name)
               and (actor_id is None or record.actor_id == actor_id)
               and (actor_type is None or record.actor_type == actor_type)
           ]
           matches.sort(key=lambda record: record.occurred_at, reverse=True)
           return matches if limit is None else matches[:limit]

       async def summarize_stages(
           self, *, task_name: str | None = None
       ) -> list[QueueEventStageSummary]:
           """Aggregate this tenant's records by stage."""
           stages: dict[str | None, list[QueueEventLogRecord]] = {}
           for record in self._records:
               if task_name is None or record.task_name == task_name:
                   stages.setdefault(record.stage, []).append(record)
           return [
               QueueEventStageSummary(
                   stage=stage,
                   event_count=len(records),
                   total_duration_ms=sum(record.duration_ms or 0.0 for record in records),
                   first_event_at=min(record.occurred_at for record in records),
                   last_event_at=max(record.occurred_at for record in records),
               )
               for stage, records in stages.items()
           ]

       async def cleanup_before(self, before: datetime, *, limit: int | None = None) -> int:
           """Delete the oldest records older than ``before`` and return the count."""
           stale = sorted(
               (record for record in self._records if record.occurred_at < before),
               key=lambda record: record.occurred_at,
           )
           if limit is not None:
               stale = stale[:limit]
           deleted = {record.event_id for record in stale}
           self._records = [record for record in self._records if record.event_id not in deleted]
           return len(deleted)

You own the storage, the schema, and the query API, and you can expose scoped
aggregates the built-in store does not. ``cleanup_before`` is what
``litestar queues run-maintenance`` calls, so honor ``limit`` to keep one
invocation bounded.

Choosing between them
=====================

Extra columns give you indexed, filterable, authorization-pushdown-capable
dimensions with no protocol churn — but only equality filtering, only text
values, and only on the concrete SQLSpec store. Implementing ``QueueEventLog``
costs more code and gives you everything: any number of dimensions, any types,
your own indexes, and your own query and aggregate surface. Start with the
columns if a tenant id and an equality filter is the whole requirement; move to
the protocol when it is not.

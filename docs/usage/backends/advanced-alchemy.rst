========================
Advanced Alchemy backend
========================

Use this backend when the application already owns SQLAlchemy models and
migrations through Advanced Alchemy.

Install and configure
=====================

.. code-block:: bash

   pip install "litestar-queues[advanced-alchemy]" aiosqlite

.. code-block:: python

   from advanced_alchemy.extensions.litestar import SQLAlchemyAsyncConfig
   from litestar_queues import QueueConfig
   from litestar_queues.backends.advanced_alchemy import SQLAlchemyBackendConfig

   alchemy = SQLAlchemyAsyncConfig(
       connection_string="sqlite+aiosqlite:///queue.db",
       create_all=True,
   )
   queue_config = QueueConfig(
       queue_backend=SQLAlchemyBackendConfig(
           sqlalchemy_config=alchemy,
       ),
       execution_backend="local",
   )

The backend opens a new database session for each queue operation. It commits
or rolls back its own changes and never keeps a request-scoped ``db_session``.
Add the same ``SQLAlchemyAsyncConfig`` to the application's
``SQLAlchemyPlugin``. The queue backend is not a schema manager.

Own the models and migrations
=============================

For production, combine ``QueueTaskModelMixin`` with the application's
declarative base. ``model_class`` is the mapped queue-task model class, not a
table-name string. Its ``__tablename__`` is the name of the queue task table.
Import that model into the application's metadata and let Advanced Alchemy
``create_all`` or Alembic migrations create it.

The built-in models use ``queue_task`` for queue records and
``queue_task_event_history`` for event history. Use the same pattern for
custom models:

.. code-block:: python

   from advanced_alchemy.base import UUIDAuditBase
   from litestar_queues.backends.advanced_alchemy import (
       SQLAlchemyBackendConfig,
       QueueEventHistoryModelMixin,
       QueueTaskModelMixin,
   )

   class AppQueueTask(UUIDAuditBase, QueueTaskModelMixin):
       __tablename__ = "app_queue_task"

   class AppQueueEventLog(UUIDAuditBase, QueueEventHistoryModelMixin):
       __tablename__ = "app_queue_task_event_log"

   queue_config = QueueConfig(
       queue_backend=SQLAlchemyBackendConfig(
           sqlalchemy_config=alchemy,
           model_class=AppQueueTask,
           event_history_model_class=AppQueueEventLog,
       ),
   )

The ``_event_history`` suffix keeps the two table names together. The queue
backend checks the model shape, but it does not create either table. Creating
them is your job: in production, generate an Alembic migration for the two new
models the same way you would for any other application table. For a local
bootstrap, create them directly from the shared metadata:

.. code-block:: python

   import asyncio


   async def create_queue_tables() -> None:
       async with alchemy.get_engine().begin() as connection:
           await connection.run_sync(UUIDAuditBase.metadata.create_all)


   asyncio.run(create_queue_tables())

``alchemy`` and ``UUIDAuditBase`` come from the example above. This creates
every model registered on that base, not only the queue tables, so use it for
development databases rather than as a substitute for migrations.

If the application uses forever uniqueness or bounded maintenance, its metadata
and migrations must also include ``QueueTaskReservationModel`` and
``QueueMaintenanceModel`` respectively, or application models composed
from the corresponding mixins. Pass custom models through
``task_reservation_model_class`` and ``maintenance_model_class``. See
:doc:`../migration` and :doc:`../maintenance` for their lifecycle contracts.

Event history uses a separate concrete model. Compose
``QueueEventHistoryModelMixin`` with the same application base and pass it as
``event_history_model_class``. Enable recording with
``QueueConfig(events=QueueEventsConfig(history=EventHistoryConfig(...)))``. A custom event model
must expose the columns required by the mixin contract and belong to the same
database lifecycle as the queue model.

Wakeups and heartbeats
======================

Wakeups are off by default here. Set ``worker_wakeups=True`` to shorten worker
pickup time on PostgreSQL; it takes effect on ``postgresql+asyncpg`` and
``postgresql+psycopg`` and is ignored everywhere else, where workers keep
polling the task table. The hint carries no task payload and is not durable, so
losing one only delays a pickup -- it never loses work.

``heartbeat_session_maker`` may use a separate session for heartbeat writes.
The application owns and closes its engine.

The full comparison against the other backends is in :doc:`../backends`; see
:ref:`worker-wakeups` for what a wakeup does and :doc:`../event-streams` for
sending events to browsers, which wakeups never do.

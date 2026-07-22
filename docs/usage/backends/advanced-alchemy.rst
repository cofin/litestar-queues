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

The built-in models use ``litestar_queue_task`` for queue records and
``litestar_queue_task_event_log`` for event history. Use the same pattern for
custom models:

.. code-block:: python

   from advanced_alchemy.base import UUIDAuditBase
   from litestar_queues.backends.advanced_alchemy import (
       SQLAlchemyBackendConfig,
       QueueEventLogModelMixin,
       QueueTaskModelMixin,
   )

   class AppQueueTask(UUIDAuditBase, QueueTaskModelMixin):
       __tablename__ = "app_queue_task"

   class AppQueueEventLog(UUIDAuditBase, QueueEventLogModelMixin):
       __tablename__ = "app_queue_task_event_log"

   queue_config = QueueConfig(
       queue_backend=SQLAlchemyBackendConfig(
           sqlalchemy_config=alchemy,
           model_class=AppQueueTask,
           event_log_model_class=AppQueueEventLog,
       ),
   )

The ``_event_log`` suffix keeps the two table names together. The queue
backend checks the model shape, but it does not create either table.

If the application uses forever uniqueness or bounded maintenance, its metadata
and migrations must also include ``QueueUniquenessModel`` and
``QueueMaintenanceLeaseModel`` respectively, or application models composed
from the corresponding mixins. Pass custom models through
``uniqueness_model_class`` and ``maintenance_lease_model_class``. See
:doc:`../migration` and :doc:`../maintenance` for their lifecycle contracts.

Event history uses a separate concrete model. Compose
``QueueEventLogModelMixin`` with the same application base and pass it as
``event_log_model_class``. Enable recording with
``QueueConfig(event_log=EventLogConfig(...))``. A custom event model
must expose the columns required by the mixin contract and belong to the same
database lifecycle as the queue model.

Wakeups and heartbeats
======================

``notifications=True`` enables direct PostgreSQL wakeup hints when the dialect
supports them. The task table remains the source of truth, so workers keep
polling. ``heartbeat_session_maker`` may use a separate session for heartbeat
writes. The application owns and closes its engine.

This direct PostgreSQL hint is not task storage and not browser delivery. See
:doc:`../worker-wakeups` and :doc:`../event-streams`.

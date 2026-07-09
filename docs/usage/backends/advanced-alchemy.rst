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
   from litestar_queues.backends.advanced_alchemy import AdvancedAlchemyBackendConfig

   alchemy = SQLAlchemyAsyncConfig(
       connection_string="sqlite+aiosqlite:///queue.db"
   )
   queue_config = QueueConfig(
       queue_backend=AdvancedAlchemyBackendConfig(
           sqlalchemy_config=alchemy,
           create_schema=False,
       ),
       execution_backend="local",
   )

The backend opens fresh operation-scoped sessions and commits or rolls back its
own queue mutations. It never retains a request-scoped ``db_session``.

Own the models and migrations
=============================

For production, compose ``QueueTaskModelMixin`` with the application's
declarative base, set ``__tablename__``, and pass the concrete class through
``model_class``. Import that model into the application's Alembic metadata.
``create_schema=True`` is only a local/test bootstrap shortcut.

Event history uses a separate concrete model. Compose
``QueueEventLogModelMixin`` with the same application base and pass it as
``event_log_model_class``. Enable recording with
``QueueConfig(event_log=EventLogConfig(...))``. A custom event model
must expose the columns required by the mixin contract and belong to the same
database lifecycle as the queue model.

Wakeups and heartbeats
======================

``notifications=True`` enables direct PostgreSQL notification hints when the
dialect supports them. The task table remains authoritative and workers keep
polling. ``heartbeat_session_maker`` may isolate heartbeat-only writes; the
application owns and disposes its engine.

This direct PostgreSQL hint is not task storage and not browser delivery. See
:doc:`../worker-wakeups` and :doc:`../event-streams`.

===============
SQLSpec backend
===============

Use SQLSpec when the application already uses it or needs SQL queue storage
without an object-relational mapper (ORM).

Install and configure
=====================

Install the queue extra and the SQLSpec driver for your database. This SQLite
example is suitable for local development:

.. code-block:: bash

   pip install "litestar-queues[sqlspec]" aiosqlite

.. code-block:: python

   from litestar import Litestar
   from sqlspec.adapters.aiosqlite import AiosqliteConfig
   from litestar_queues import QueueConfig, QueuePlugin
   from litestar_queues.backends.sqlspec import SQLSpecBackendConfig

   sqlspec_config = AiosqliteConfig(connection_config={"database": "queue.db"})

   queue_config = QueueConfig(
       queue_backend=SQLSpecBackendConfig(
           config=sqlspec_config,
       ),
       execution_backend="local",
   )
   app = Litestar(plugins=[QueuePlugin(queue_config)])

   # Run this from the app's normal SQLSpec migration command or deploy step.
   await sqlspec_config.migrate_up(echo=False)

The supplied SQLSpec config selects the adapter. ``QueuePlugin`` registers the
queue migration during app and CLI initialization; SQLSpec then runs it through
the same migration command used by the rest of the application. The queue
backend does not migrate the database when it opens. An unsupported adapter
raises a configuration error instead of silently using generic SQL.

Schema ownership
================

Packaged migrations run through SQLSpec's extension system. Do not replace the
application's migration ``script_location``. When migrations run outside the
Litestar app, call ``configure_queue_migration_extension(sqlspec_config)``
before the normal SQLSpec migration command. If the application owns an
existing table, set ``manage_schema=False``. Map that table with
``queue_table_name``, ``column_map``, and ``native_json_columns``.

For a small local bootstrap without a migration command, call the backend's
explicit ``create_schema()`` operation after ``open()``. This emits adapter-
specific DDL directly and does not record a migration revision; it is a
development fallback, not a replacement for application migrations.

The default queue table is ``litestar_queue_task``. When event history is
enabled, SQLSpec derives its table by adding ``_event_log`` to the queue table,
so the default is ``litestar_queue_task_event_log``. Set
``event_log_table_name`` only when the application needs a different name.
Schema-qualified names keep their schema and add the suffix to the table part.

Wakeups
-------

Set ``notifications`` and ``notify_transport`` only for a SQLSpec transport
supported by the selected adapter. ``notification_channel`` defaults to
``litestar_queues_tasks``. Canonical PostgreSQL-family choices are ``notify``
(ephemeral broadcast hint), ``notify_queue`` (durable notify-assisted queue),
and ``poll_queue`` (durable polled queue). ``polling`` disables push wakeups;
Oracle can use explicitly provisioned ``aq`` or ``txeventq``. Durable queue
transports are competing-consumer queues; do not use them as multi-process
browser fan-out. See :doc:`../worker-wakeups`.

Heartbeat isolation
===================

``heartbeat_pool_config`` can use a dedicated connection pool for heartbeat
writes. It must point to the same database. The backend opens and closes that
pool, and falls back to the main pool if registration fails.

Event history
=============

SQLSpec event history uses the queue schema, the packaged SQLSpec migration,
and the SQLSpec session lifecycle. Its table naming follows the queue-table
``_event_log`` suffix described above. ``event_log_table_name`` customizes the
table.
Live SSE/WebSocket delivery still needs a Channels backend. See
:doc:`../event-history`.

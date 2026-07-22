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

.. note:: Performance

   SQLSpec's ``sqlspec[performance]`` extra (msgspec/librt serialization) and
   ``sqlspec[mypyc]`` extra (compiled SQL parser) speed up statement handling.
   These are consumer choices; litestar-queues does not require or install
   them on your behalf.


.. warning::

   The ``mssql-python`` adapter is temporarily unsupported with SQLSpec 0.55.0
   because its transaction path can report a successful commit while discarding
   queue writes. Litestar Queues rejects that adapter at configuration time;
   use ``pymssql`` or ``arrow-odbc`` for SQL Server until
   `SQLSpec issue 642 <https://github.com/litestar-org/sqlspec/issues/642>`_
   is fixed upstream.

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

Native worker wakeups are **on by default whenever the adapter can push them**.
A bare ``SQLSpecBackendConfig`` needs no notification settings: the backend
selects each adapter's best wakeup transport from a capability gate and
provisions everything it needs.

.. list-table::
   :header-rows: 1
   :widths: 30 25 45

   * - Adapter
     - Default transport
     - Wakeup mechanism
   * - asyncpg, psycopg, psqlpy
     - ``notify_queue``
     - Durable events queue table + PostgreSQL ``LISTEN``/``NOTIFY`` push
   * - DuckDB
     - ``poll_queue``
     - Durable events queue table, polled in-process (embedded, no LISTEN/NOTIFY)
   * - SQLite, MySQL, CockroachDB, SQL Server, Spanner, Oracle
     - ``polling``
     - Interval polling (no push transport available by default)

The durable ``notify_queue`` and ``poll_queue`` transports ride a SQLSpec events
queue table (``sqlspec_event_queue`` by default). It is provisioned the same way
as the queue table: the packaged migration path registers SQLSpec's events
migration for capable adapters automatically, and the ``create_schema()``
bootstrap emits its DDL directly. A zero-config capable backend therefore works
on a fresh database with no manual step. ``event_queue_table`` overrides the
table name.

To turn native wakeups off and fall back to interval polling, set
``notifications=False``. Setting ``notifications=True`` on an adapter that has no
push transport degrades to polling rather than forcing an unsupported one.

Overrides remain available for advanced setups: ``notify_transport`` pins a
specific SQLSpec transport, ``notification_channel`` sets the LISTEN/NOTIFY
channel (default ``litestar_queues_tasks``), and Oracle can opt in to explicitly
provisioned ``aq`` or ``txeventq`` queues. Oracle stays on polling by default
because Advanced Queuing requires provisioning that the backend does not create.
Durable queue transports are competing-consumer queues; do not use them as
multi-process browser fan-out. See :doc:`../worker-wakeups`.

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

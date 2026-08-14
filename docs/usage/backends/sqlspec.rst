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
           sqlspec_config=sqlspec_config,
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

Typical PostgreSQL pickup time
==============================

On a local development machine with an idle worker already running, 95 out of
100 tasks started within about **5.5 ms with asyncpg** and **9.5 ms with
psycopg**. Other SQLSpec adapters use different notification or polling paths,
so do not apply these PostgreSQL times to SQLite, MySQL, Oracle, or other
databases. Network distance and database load can also increase pickup time.


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

The default queue table is ``queue_task``. When event history is enabled,
SQLSpec derives its table by adding ``_event_history`` to the queue table,
so the default is ``queue_task_event_history``. Set
``event_history_table_name`` only when the application needs a different name.
The single packaged ``0001_create_queue_tasks`` migration creates the queue
task table, enabled event history, ``queue_maintenance`` for distributed
maintenance coordination, and ``queue_task_reservation`` for permanent task
identity reservations. Override the names with ``maintenance_table_name`` and
``task_reservation_table_name``. Schema-qualified custom queue names keep their
schema and add the corresponding suffix to the table part.
See :doc:`../maintenance` before scheduling maintenance and
:doc:`../migration` before using forever uniqueness.

Wakeups
-------

Native worker wakeups are **on by default whenever the adapter can push them**.
A bare ``SQLSpecBackendConfig`` needs no notification settings: the backend
picks the transport from the adapter you configured and provisions everything
it needs. In short, the PostgreSQL drivers (asyncpg, psycopg, psqlpy) get
``notify_queue``, DuckDB gets ``poll_queue``, and every other adapter --
SQLite, MySQL, CockroachDB, SQL Server, Spanner, and Oracle -- falls back to
interval polling. :doc:`../backends` carries the full matrix, including how
these compare with the other queue backends.

The durable ``notify_queue`` and ``poll_queue`` transports ride a SQLSpec events
queue table (``sqlspec_event_queue`` by default). It is provisioned the same way
as the queue table: the packaged migration path registers SQLSpec's events
migration for capable adapters automatically, and the ``create_schema()``
bootstrap emits its DDL directly. A zero-config capable backend therefore works
on a fresh database with no manual step. Set
``SQLSpecWorkerWakeupConfig.queue_table_name`` to override the events queue
table name.

To turn native wakeups off and fall back to interval polling, set
``worker_wakeups=None``. The default ``SQLSpecWorkerWakeupConfig()`` selects the
adapter capability; an adapter without a push transport continues interval
polling.

Overrides remain available under ``worker_wakeups``: ``transport`` pins a
specific native SQLSpec transport. The canonical names are ``notify``,
``notify_queue``, ``poll_queue``, ``aq``, and ``txeventq``. ``channel_name``
sets the LISTEN/NOTIFY channel (default ``litestar_queues_tasks``), and
``poll_interval`` controls durable queue polling in seconds. Oracle can opt in
to explicitly provisioned ``aq`` or ``txeventq`` queues through ``transport``
and ``settings``. The application or database operator owns queue creation,
startup, privileges, payload type, and retention; Litestar Queues only opens
the configured SQLSpec event channel. Oracle stays on interval polling by
default because the backend does not provision Advanced Queuing.
Durable queue transports are competing-consumer queues; do not use them as
multi-process browser fan-out. See :ref:`worker-wakeups`.

Batch claiming
==============

SQLSpec stores that advertise a returning claim use one bounded
``UPDATE ... RETURNING`` operation for ``claim_many``. Stores without that
primitive inherit the backend-neutral single-record claim loop. Both paths
preserve the same priority, filtering, ownership, and actual-short-batch
contract; only the number of database operations differs.

Heartbeat isolation
===================

``heartbeat_pool_config`` can use a dedicated connection pool for heartbeat
writes. It must point to the same database. The backend opens and closes that
pool, and falls back to the main pool if registration fails.

Event history
=============

SQLSpec event history uses the queue schema, the packaged SQLSpec migration,
and the SQLSpec session lifecycle. Its table naming follows the queue-table
``_event_history`` suffix described above. ``event_history_table_name`` customizes the
table.
Live SSE/WebSocket delivery still needs a Channels backend. See
:doc:`../event-history`.

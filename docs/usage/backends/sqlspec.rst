===============
SQLSpec backend
===============

Use SQLSpec when the application already uses SQLSpec or needs queue
persistence across its supported adapter families without adopting an ORM.

Install and configure
=====================

Install the queue extra and the SQLSpec driver for your database. This SQLite
example is suitable for local development:

.. code-block:: bash

   pip install "litestar-queues[sqlspec]" aiosqlite

.. code-block:: python

   from sqlspec.adapters.aiosqlite import AiosqliteConfig
   from litestar_queues import QueueConfig
   from litestar_queues.backends.sqlspec import SQLSpecBackendConfig

   queue_config = QueueConfig(
       queue_backend=SQLSpecBackendConfig(
           config=AiosqliteConfig(connection_config={"database": "queue.db"}),
           run_migrations=True,
       ),
       execution_backend="local",
       in_app_worker=False,
   )

Adapter selection follows the supplied SQLSpec config. Unsupported adapters
raise a configuration error instead of silently using a generic SQL path.

Schema ownership
================

Packaged migrations run as SQLSpec extension migrations. Do not replace the
application migration ``script_location``. Set ``manage_schema=False`` when
the application owns an existing table; use ``table_name``, ``column_map``,
and ``native_json_columns`` to map it safely.

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

``heartbeat_pool_config`` can route heartbeat-only writes through a dedicated
pool pointing at the same database. The backend owns that pool's open/close
lifecycle and falls back to the main pool if registration fails.

Event history
=============

SQLSpec event history uses the queue schema, packaged migration path, and
SQLSpec session lifecycle. ``event_log_table_name`` customizes the table.
Live SSE/WebSocket delivery still needs a Channels backend. See
:doc:`../event-history`.

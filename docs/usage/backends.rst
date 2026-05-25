Backends
========

Litestar Queues separates queue backends from execution backends.

Queue backends persist task state. The core package registers the ``memory``
backend for tests, local development, and in-process workers. The ``sqlspec``
backend is available when the SQLSpec extra is installed. Additional optional
extras provide Advanced Alchemy, Redis, and Valkey integrations.

Execution backends decide where claimed tasks run. The core package registers
``immediate`` for inline execution and ``local`` for in-process worker
execution. The optional ``cloudrun`` extra dispatches records to Cloud Run Jobs.

Install optional extras only when an application needs them:

.. code-block:: bash

   pip install litestar-queues[sqlspec]
   pip install litestar-queues[advanced-alchemy]
   pip install litestar-queues[redis]
   pip install litestar-queues[valkey]
   pip install litestar-queues[cloudrun]

The core package import does not require optional queue or execution client
libraries.

SQLSpec
-------

Install the SQLSpec extra when a queue needs SQL-backed persistence:

.. code-block:: bash

   pip install litestar-queues[sqlspec]

Configure SQLSpec queue persistence by passing a SQLSpec adapter config through
``SQLSpecBackendConfig``:

.. code-block:: python

   from sqlspec.adapters.aiosqlite import AiosqliteConfig

   from litestar_queues import QueueConfig
   from litestar_queues.backends.sqlspec import SQLSpecBackendConfig

   config = QueueConfig(
       queue_backend=SQLSpecBackendConfig(
           sqlspec_config=AiosqliteConfig(
               connection_config={"database": "queue.db"},
           ),
       ),
       execution_backend="local",
   )

By default, the backend creates the queue table on startup. Set
``create_schema=False`` in ``SQLSpecBackendConfig`` when schema management is
handled elsewhere. Applications that want SQLSpec to apply the packaged queue
migration can set ``run_migrations=True``:

.. code-block:: python

   config = QueueConfig(
       queue_backend=SQLSpecBackendConfig(
           sqlspec_config=AiosqliteConfig(
               connection_config={"database": "queue.db"},
           ),
           create_schema=False,
           run_migrations=True,
       ),
       execution_backend="local",
   )

Litestar applications that use SQLSpec sessions should register SQLSpec's
first-party plugin directly and pass the same ``SQLSpec`` instance and adapter
config to the queue backend:

.. code-block:: python

   from litestar import Litestar
   from sqlspec import SQLSpec
   from sqlspec.adapters.aiosqlite import AiosqliteConfig
   from sqlspec.extensions.litestar import SQLSpecPlugin

   from litestar_queues import QueueConfig, QueuePlugin
   from litestar_queues.backends.sqlspec import SQLSpecBackendConfig

   sqlspec = SQLSpec()
   sqlspec_config = AiosqliteConfig(connection_config={"database": "queue.db"})
   sqlspec.add_config(sqlspec_config)

   app = Litestar(
       plugins=[
           SQLSpecPlugin(sqlspec),
           QueuePlugin(
               QueueConfig(
                   queue_backend=SQLSpecBackendConfig(
                       sqlspec=sqlspec,
                       sqlspec_config=sqlspec_config,
                   ),
                   execution_backend="local",
               )
           )
       ],
   )

SQLSpec persists task arguments, keyword arguments, metadata, and results using
SQLSpec's serializer. Packaged migrations are registered with SQLSpec's
extension runner as ``ext_litestar_queues_0001``. Applications with their own
migration flow can set both ``create_schema=False`` and ``run_migrations=False``.

.. _heartbeat-pool-isolation:

Heartbeat Pool Isolation
~~~~~~~~~~~~~~~~~~~~~~~~

Workers issue a heartbeat write every ``QueueConfig.worker_heartbeat_interval``
seconds for every running task. At high ``worker_max_concurrency`` those writes
share the main pool with task fetch, claim, and lifecycle UPDATEs, and on
network databases (AsyncPG, AioMySQL) heartbeats can stall behind queue work
and miss the stale-recovery window.

Set ``heartbeat_pool_config`` to a second SQLSpec adapter config so heartbeat
writes run on a small dedicated pool:

.. code-block:: python

   from sqlspec.adapters.asyncpg import AsyncpgConfig

   from litestar_queues import QueueConfig
   from litestar_queues.backends.sqlspec import SQLSpecBackendConfig

   queue_url = "postgresql://queue@db/queues"

   main_config = AsyncpgConfig(
       pool_config={"dsn": queue_url, "min_size": 4, "max_size": 16},
   )
   heartbeat_config = AsyncpgConfig(
       pool_config={"dsn": queue_url, "min_size": 1, "max_size": 2},
   )

   config = QueueConfig(
       queue_backend=SQLSpecBackendConfig(
           sqlspec_config=main_config,
           heartbeat_pool_config=heartbeat_config,
       ),
       execution_backend="local",
       worker_max_concurrency=32,
   )

The dedicated config MUST point at the same database as the main config; the
backend only uses it for ``touch_heartbeat`` and ``null_heartbeats``. Lifecycle
writes that touch ``heartbeat_at`` alongside other columns (``claim_task``,
``complete_task``, ``fail_task``, ``requeue_stale_running``) stay on the main
pool. Recommended sizing: ``max_size=2`` for AsyncPG / AioMySQL,
``max_size=1`` for AioSQLite. The dedicated pool's connections add to the
application's total database connection budget.

When ``heartbeat_pool_config`` is ``None`` (the default), heartbeat writes
share the main pool exactly as before. If the dedicated pool fails to register
or open, the backend logs a single warning and falls back to the main pool for
the lifetime of the backend.

SQLSpec Store Selection
~~~~~~~~~~~~~~~~~~~~~~~

The SQLSpec queue backend selects stores by SQLSpec adapter configuration, not
by directly importing database drivers. This keeps driver dependencies optional
and lets each store use database-specific DDL, column types, JSON behavior, and
claim/update statements where the database supports them.

.. list-table::
   :header-rows: 1

   * - SQLSpec adapter
     - Queue store
     - Notes
   * - ``adbc``
     - ``AdbcQueueStore``
     - Shared ADBC behavior.
   * - ``aiomysql``
     - ``AiomysqlQueueStore``
     - Async MySQL behavior.
   * - ``aiosqlite``
     - ``AiosqliteQueueStore``
     - Async SQLite behavior.
   * - ``asyncmy``
     - ``AsyncmyQueueStore``
     - Async MySQL behavior.
   * - ``asyncpg``
     - ``AsyncpgQueueStore``
     - PostgreSQL behavior.
   * - ``bigquery``
     - ``BigQueryQueueStore``
     - BigQuery DDL and JSON behavior.
   * - ``cockroach_asyncpg``
     - ``CockroachAsyncpgQueueStore``
     - CockroachDB through asyncpg.
   * - ``cockroach_psycopg``
     - ``CockroachPsycopgAsyncQueueStore`` or ``CockroachPsycopgSyncQueueStore``
     - Async and sync variants are selected from the SQLSpec config type.
   * - ``duckdb``
     - ``DuckDBQueueStore``
     - DuckDB-specific DDL and JSON behavior.
   * - ``mysqlconnector``
     - ``MysqlConnectorAsyncQueueStore`` or ``MysqlConnectorSyncQueueStore``
     - Async and sync variants are selected from the SQLSpec config type.
   * - ``oracledb``
     - ``OracledbAsyncQueueStore`` or ``OracledbSyncQueueStore``
     - Uses Oracle-specific DDL and JSON column choices.
   * - ``psqlpy``
     - ``PsqlpyQueueStore``
     - PostgreSQL behavior.
   * - ``psycopg``
     - ``PsycopgAsyncQueueStore`` or ``PsycopgSyncQueueStore``
     - Async and sync variants are selected from the SQLSpec config type.
   * - ``pymysql``
     - ``PymysqlQueueStore``
     - Sync MySQL behavior.
   * - ``spanner``
     - ``SpannerQueueStore``
     - Spanner-specific DDL and JSON behavior.
   * - ``sqlite``
     - ``SqliteQueueStore``
     - Sync SQLite behavior.

Unsupported adapters fall back to the shared SQLSpec store. Applications should
install the SQLSpec adapter driver they configure; Litestar Queues does not
install every SQLSpec driver as a package dependency.

Advanced Alchemy
----------------

Install the Advanced Alchemy extra when a queue should persist task state using
Advanced Alchemy and SQLAlchemy:

.. code-block:: bash

   pip install litestar-queues[advanced-alchemy]

Configure the queue backend with an app-owned queue model and
``SQLAlchemyAsyncConfig``:

.. code-block:: python

   from advanced_alchemy.base import UUIDAuditBase
   from advanced_alchemy.extensions.litestar import SQLAlchemyAsyncConfig

   from litestar_queues import QueueConfig
   from litestar_queues.backends.advanced_alchemy import AdvancedAlchemyBackendConfig, QueueTaskModelMixin

   class AppQueueTask(UUIDAuditBase, QueueTaskModelMixin):
       __tablename__ = "app_queue_tasks"

   alchemy_config = SQLAlchemyAsyncConfig(
       connection_string="sqlite+aiosqlite:///queue.db",
   )

   config = QueueConfig(
       queue_backend=AdvancedAlchemyBackendConfig(
           sqlalchemy_config=alchemy_config,
           model_class=AppQueueTask,
           create_schema=True,
       ),
       execution_backend="local",
   )

Queue operations use fresh operation-scoped sessions opened from
``sqlalchemy_config`` and commit or roll back queue mutations explicitly.

Litestar applications should register Advanced Alchemy's first-party plugin
directly and pass the same config to the queue backend:

.. code-block:: python

   from advanced_alchemy.base import UUIDAuditBase
   from advanced_alchemy.extensions.litestar import SQLAlchemyAsyncConfig, SQLAlchemyPlugin
   from litestar import Litestar

   from litestar_queues import QueueConfig, QueuePlugin
   from litestar_queues.backends.advanced_alchemy import AdvancedAlchemyBackendConfig, QueueTaskModelMixin

   class AppQueueTask(UUIDAuditBase, QueueTaskModelMixin):
       __tablename__ = "app_queue_tasks"

   alchemy_config = SQLAlchemyAsyncConfig(
       connection_string="sqlite+aiosqlite:///queue.db",
   )

   app = Litestar(
       plugins=[
           SQLAlchemyPlugin(config=alchemy_config),
           QueuePlugin(
               QueueConfig(
                   queue_backend=AdvancedAlchemyBackendConfig(
                       sqlalchemy_config=alchemy_config,
                       model_class=AppQueueTask,
                       create_schema=True,
                   ),
                   execution_backend="local",
               )
           ),
       ],
   )

The queue plugin does not append ``SQLAlchemyPlugin`` or consume request-scoped
``db_session`` dependencies. Applications that manage schema with Alembic should
import the queue model they use into their Alembic environment so autogenerate
can include the queue table in the application migration stream.

App-Owned Queue Model
~~~~~~~~~~~~~~~~~~~~~

Advanced Alchemy support uses only an application-owned model. Compose
``QueueTaskModelMixin`` with an Advanced Alchemy base that provides compatible
``id`` and ``created_at`` columns, then pass the resulting model class to the
backend:

.. code-block:: python

   from advanced_alchemy.base import UUIDAuditBase
   from advanced_alchemy.extensions.litestar import SQLAlchemyAsyncConfig

   from litestar_queues import QueueConfig
   from litestar_queues.backends.advanced_alchemy import AdvancedAlchemyBackendConfig, QueueTaskModelMixin

   class AppQueueTask(UUIDAuditBase, QueueTaskModelMixin):
       __tablename__ = "app_queue_tasks"

   alchemy_config = SQLAlchemyAsyncConfig(
       connection_string="sqlite+aiosqlite:///queue.db",
   )

   config = QueueConfig(
       queue_backend=AdvancedAlchemyBackendConfig(
           sqlalchemy_config=alchemy_config,
           model_class=AppQueueTask,
           create_schema=True,
       ),
       execution_backend="local",
   )

``QueueTaskModelMixin`` carries the queue columns and derives index names from
the composed class's ``__tablename__``. If your application uses Alembic
autogenerate, import this model in ``env.py`` and include its metadata in
``target_metadata``. Use ``create_schema`` only for local bootstrap or tests;
production schema changes should be generated and reviewed in the application
migration stream.

.. _aa-heartbeat-session-maker:

Heartbeat Session Maker Isolation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Workers issue a heartbeat write every ``QueueConfig.worker_heartbeat_interval``
seconds for every running task. At high ``worker_max_concurrency`` those writes
share the main async SQLAlchemy pool with task fetch, claim, and lifecycle
UPDATEs. On network databases (AsyncPG, AioMySQL) heartbeats can stall behind
queue work and miss the stale-recovery window.

Construct a dedicated async engine and ``async_sessionmaker``, then pass it
through ``heartbeat_session_maker``:

.. code-block:: python

   from advanced_alchemy.extensions.litestar import SQLAlchemyAsyncConfig
   from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

   from litestar_queues import QueueConfig
   from litestar_queues.backends.advanced_alchemy import AdvancedAlchemyBackendConfig
   from myapp.models import AppQueueTask

   queue_url = "postgresql+asyncpg://queue@db/queues"

   main_config = SQLAlchemyAsyncConfig(connection_string=queue_url)
   heartbeat_engine = create_async_engine(queue_url, pool_size=1, max_overflow=1)
   heartbeat_maker = async_sessionmaker(heartbeat_engine, expire_on_commit=False)

   config = QueueConfig(
       queue_backend=AdvancedAlchemyBackendConfig(
           sqlalchemy_config=main_config,
           model_class=AppQueueTask,
           heartbeat_session_maker=heartbeat_maker,
       ),
       execution_backend="local",
       worker_max_concurrency=32,
   )

The dedicated engine MUST point at the same database as the main config. The
backend uses it only for ``touch_heartbeat`` and ``null_heartbeats``; lifecycle
UPDATEs that touch ``heartbeat_at`` alongside other columns (``claim_task``,
``complete_task``, ``fail_task``, ``requeue_stale_running``) stay on the main
session for transactional correctness. Recommended sizing:
``pool_size=1, max_overflow=1`` for AsyncPG / AioMySQL, ``pool_size=1`` for
``aiosqlite`` (SQLite serializes writes anyway). The dedicated engine's
connections add to the application's total database connection budget.

The adopter owns the dedicated engine's lifecycle. ``backend.close()`` does
NOT dispose the heartbeat engine — call ``engine.dispose()`` from the
application shutdown hook that owns the engine. Unlike the SQLSpec backend
(which registers its dedicated config on ``open()``), the Advanced Alchemy
backend never constructs or owns the heartbeat engine; any construction error
surfaces from the first ``touch_heartbeat()`` call.

When ``heartbeat_session_maker`` is ``None`` (the default), heartbeat writes
share the main session exactly as before.

Redis
-----

Install the Redis extra when a queue should persist task state in Redis:

.. code-block:: bash

   pip install litestar-queues[redis]

Configure the queue backend with a Redis URL or pass an already configured
async Redis client:

.. code-block:: python

   from litestar_queues import QueueConfig
   from litestar_queues.backends.redis import RedisBackendConfig

   config = QueueConfig(
       queue_backend=RedisBackendConfig(
           url="redis://localhost:6379/0",
           key_prefix="litestar_queues",
           notifications=True,
       ),
       execution_backend="local",
   )

Redis queue records are stored in hashes under the configured key prefix. The
backend keeps an ID set for operational queries, a key hash for deduplication,
a sorted set for delayed scheduling, and short-lived ``SET NX`` locks around
claim and key-replacement mutations. Task arguments, keyword arguments,
metadata, and results must be JSON serializable.

Redis pub/sub is used only as a worker wakeup mechanism. Notifications are not
durable; workers that miss a message fall back to polling.

Valkey
------

Install the Valkey extra when a queue should use Valkey's asyncio client:

.. code-block:: bash

   pip install litestar-queues[valkey]

Configure Valkey with the same queue backend settings:

.. code-block:: python

   from litestar_queues import QueueConfig
   from litestar_queues.backends.valkey import ValkeyBackendConfig

   config = QueueConfig(
       queue_backend=ValkeyBackendConfig(
           url="redis://localhost:6379/0",
           key_prefix="litestar_queues",
           notifications=True,
       ),
       execution_backend="local",
   )

Valkey follows the same queue lifecycle contract as Redis: active key
deduplication, terminal key replacement, delayed scheduling, atomic claim via
backend locks, retries, heartbeats, stale running recovery, result lookup,
stats, cleanup, and optional pub/sub worker wakeups.

Cloud Run
---------

Install the Cloud Run extra when tasks should execute in Cloud Run Jobs:

.. code-block:: bash

   pip install litestar-queues[cloudrun]

Configure execution with generic package settings. Queue persistence remains
app-owned and can use any queue backend that supports execution references:

.. code-block:: python

   from litestar_queues import QueueConfig, task
   from litestar_queues.backends.sqlspec import SQLSpecBackendConfig
   from litestar_queues.execution.cloudrun import CloudRunExecutionConfig

   @task("reports.render", execution_backend="cloudrun", execution_profile="heavy")
   async def render_report(report_id: str) -> None:
       ...

   config = QueueConfig(
       queue_backend=SQLSpecBackendConfig(sqlspec_config=...),
       execution_backend=CloudRunExecutionConfig(
           project_id="example-project",
           region="us-central1",
           job_name="queue-worker",
           profiles={"heavy": "queue-worker-heavy"},
       ),
   )

The dispatch worker stores the Cloud Run execution name on the queue record and
does not claim the task locally. The Cloud Run container should run
``litestar-queues-cloudrun-worker`` or
``python -m litestar_queues.execution.cloudrun.entrypoint``. The entry point
reads ``LITESTAR_QUEUES_TASK_ID``, loads configured task modules, claims the
persisted record, updates heartbeats, executes the task through the shared queue
service, and returns deterministic process exit codes.

If the Cloud Run API call fails before a remote execution owns the task, the
backend can move the record to a fallback execution backend such as ``local``.
Status checks that fail transiently are treated as still running so
reconciliation does not create false terminal states.

SQLSpec Event Notifications
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The SQLSpec backend falls back to worker polling unless notifications are
configured. To wake workers through SQLSpec Events, configure the SQLSpec
``events`` extension and enable queue notifications:

.. code-block:: python

   from sqlspec.adapters.aiosqlite import AiosqliteConfig

   from litestar_queues import QueueConfig
   from litestar_queues.backends.sqlspec import SQLSpecBackendConfig

   sqlspec_config = AiosqliteConfig(
       connection_config={"database": "queue.db"},
       extension_config={
           "events": {
               "backend": "table_queue",
               "queue_table": "queue_events",
               "poll_interval": 0.1,
           }
       },
   )

   config = QueueConfig(
       queue_backend=SQLSpecBackendConfig(
           sqlspec_config=sqlspec_config,
           create_schema=False,
           run_migrations=True,
           notifications=True,
           notification_channel="queue_notifications",
       ),
       execution_backend="local",
   )

PostgreSQL SQLSpec adapters can use SQLSpec's native ``listen_notify`` backend;
other adapters can use the durable ``table_queue`` backend. Queue notification
channel names must be valid SQLSpec event identifiers.

Shared SQLSpec Extension Config
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Applications can place queue defaults in ``sqlspec_config.extension_config``.
Explicit values passed through ``SQLSpecBackendConfig`` take
precedence:

.. code-block:: python

   sqlspec_config = AiosqliteConfig(
       connection_config={"database": "queue.db"},
       extension_config={
           "litestar_queues": {
               "table_name": "app_queue_tasks",
               "notification_channel": "app_queue_notifications",
           }
       },
   )

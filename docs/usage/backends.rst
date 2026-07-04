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
           config=AiosqliteConfig(
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
           config=AiosqliteConfig(
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
                       config=sqlspec_config,
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
           config=main_config,
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
   * - ``cockroach_asyncpg``
     - ``CockroachAsyncpgQueueStore``
     - CockroachDB behavior on the asyncpg driver.
   * - ``duckdb``
     - ``DuckDBQueueStore``
     - DuckDB-specific DDL and JSON behavior.
   * - ``mysqlconnector``
     - ``MysqlConnectorAsyncQueueStore`` or ``MysqlConnectorSyncQueueStore``
     - Async and sync variants are selected from the SQLSpec config type.
   * - ``pymysql``
     - ``PymysqlQueueStore``
     - Sync MySQL behavior.
   * - ``mssql_python``
     - ``MssqlPythonQueueStore``
     - Async and sync SQL Server configs share the same queue store.
   * - ``pymssql``
     - ``PymssqlQueueStore``
     - Sync-only SQL Server adapter.
   * - ``oracledb``
     - ``OracledbAsyncQueueStore`` or ``OracledbSyncQueueStore``
     - Uses Oracle-specific DDL and JSON column choices.
   * - ``spanner``
     - ``SpannerQueueStore``
     - Google Cloud Spanner behavior with write-capable transactional sessions.
   * - ``psqlpy``
     - ``PsqlpyQueueStore``
     - PostgreSQL behavior.
   * - ``psycopg``
     - ``PsycopgAsyncQueueStore`` or ``PsycopgSyncQueueStore``
     - Async and sync variants are selected from the SQLSpec config type.
   * - ``cockroach_psycopg``
     - ``CockroachPsycopgAsyncQueueStore`` or ``CockroachPsycopgSyncQueueStore``
     - CockroachDB behavior on psycopg; async and sync variants are selected
       from the SQLSpec config type.
   * - ``sqlite``
     - ``SqliteQueueStore``
     - Sync SQLite behavior.

Unsupported SQLSpec adapters raise ``QueueConfigurationError``. Applications
should install the SQLSpec adapter driver they configure; Litestar Queues does
not install every SQLSpec driver as a package dependency.

SQLSpec Capability Matrix
~~~~~~~~~~~~~~~~~~~~~~~~~

SQLSpec adapters use the strongest queue primitive their configured driver
advertises, then fall back to the portable path when a capability is absent.

.. list-table::
   :header-rows: 1

   * - Adapter family
     - Claim strategy
     - JSON storage and codec
     - Bulk insert
     - Notifications
     - Notes
   * - ``aiosqlite`` / ``sqlite``
     - Optimistic compare-and-swap.
     - ``TEXT`` columns serialized with SQLSpec JSON.
     - Native Arrow ``load_from_records`` when SQLSpec exposes it; otherwise
       ``execute_many``.
     - Polling unless an explicit SQLSpec table queue is configured.
     - SQLite serializes writes, so the portable path is the concurrency guard.
   * - ``duckdb``
     - Optimistic compare-and-swap.
     - ``JSON`` columns serialized with SQLSpec JSON.
     - Arrow ``load_from_records`` path.
     - Polling.
     - Positional Arrow ingest is contract-tested so column order matches the
       table definition.
   * - ``asyncpg`` / ``psycopg``
     - ``FOR UPDATE SKIP LOCKED`` when SQLSpec's data dictionary marks the
       dialect support; compare-and-swap fallback otherwise.
     - ``JSONB`` with native decoded JSON columns where the driver returns
       Python values.
     - Arrow ``load_from_records`` path.
     - ``asyncpg`` uses durable LISTEN/NOTIFY; ``psycopg`` and ``psqlpy`` can
       use SQLSpec's durable table queue.
     - Stale-recovery statement batches use SQLSpec ``StatementStack``; psycopg
       can collapse the batch through the driver pipeline.
   * - ``cockroach_asyncpg`` / ``cockroach_psycopg``
     - Optimistic compare-and-swap.
     - ``JSONB`` with native decoded JSON columns where the driver returns
       Python values.
     - Arrow ``load_from_records`` path.
     - Polling.
     - CockroachDB keeps PostgreSQL-compatible DDL, but the queue backend
       stays on the portable claim path instead of relying on ``SKIP LOCKED``.
   * - ``psqlpy``
     - ``FOR UPDATE SKIP LOCKED`` when SQLSpec's data dictionary marks the
       dialect support.
     - ``JSONB`` payload/metadata columns with native decode; ``result_json``
       stays text-backed for driver compatibility.
     - Arrow ``load_from_records`` path.
     - SQLSpec durable table queue.
     - PostgreSQL storage parameters tune queue-table churn where supported.
   * - ``asyncmy`` / ``aiomysql`` / ``mysqlconnector``
     - ``FOR UPDATE SKIP LOCKED`` when SQLSpec's data dictionary marks the
       dialect support.
     - MySQL ``JSON`` columns with native decoded JSON values.
     - Arrow ``load_from_records`` path.
     - Polling.
     - Index prefixes keep InnoDB key length within portable bounds.
   * - ``pymysql``
     - ``FOR UPDATE SKIP LOCKED`` when SQLSpec's data dictionary marks the
       dialect support.
     - MySQL ``JSON`` columns with native decoded JSON values.
     - Arrow ``load_from_records`` path.
     - Polling.
     - Sync MySQL behavior uses the same InnoDB prefix guard.
   * - ``mssql_python``
     - Optimistic compare-and-swap.
     - ``NVARCHAR(MAX)`` text columns serialized with SQLSpec JSON.
     - Arrow ``load_from_records`` path.
     - Polling.
     - SQL Server support keeps the portable claim path and shares the same
       queue store across sync and async configs.
   * - ``pymssql``
     - Optimistic compare-and-swap.
     - ``NVARCHAR(MAX)`` text columns serialized with SQLSpec JSON.
     - ``execute_many`` path.
     - Polling.
     - Sync-only SQL Server adapter with the same portable claim path.
   * - ``oracledb``
     - ``FOR UPDATE SKIP LOCKED`` through SQLSpec's Oracle data-dictionary
       capability.
     - Version-aware ``JSON``, checked ``BLOB``, or plain ``BLOB`` storage.
     - Arrow ``load_from_records`` path.
     - Polling by default; explicit ``aq`` or ``txeventq`` when Oracle queues
       are provisioned.
     - Oracle object names are kept within the adapter's identifier limits;
       Oracle 23ai can pipeline stale-recovery statement batches.
   * - ``spanner``
     - Optimistic compare-and-swap.
     - ``JSON`` columns with native decoded JSON values.
     - Arrow ``load_from_records`` path.
     - Polling.
     - Spanner sessions are write-capable by default; the queue store uses
       ``STRING``/``INT64`` DDL and a ``UNIQUE NULL_FILTERED`` index for
       ``task_key``.

Additional SQLSpec adapters can be added by implementing a queue store and
registering it with the SQLSpec store factory.

Advanced Alchemy
----------------

Install the Advanced Alchemy extra when a queue should persist task state using
Advanced Alchemy and SQLAlchemy:

.. code-block:: bash

   pip install litestar-queues[advanced-alchemy]

Configure the queue backend with ``SQLAlchemyAsyncConfig``:

.. code-block:: python

   from advanced_alchemy.extensions.litestar import SQLAlchemyAsyncConfig

   from litestar_queues import QueueConfig
   from litestar_queues.backends.advanced_alchemy import AdvancedAlchemyBackendConfig

   alchemy_config = SQLAlchemyAsyncConfig(
       connection_string="sqlite+aiosqlite:///queue.db",
   )

   config = QueueConfig(
       queue_backend=AdvancedAlchemyBackendConfig(
           sqlalchemy_config=alchemy_config,
           create_schema=True,
       ),
       execution_backend="local",
   )

Queue operations use one queue table by default: ``litestar_queue_task``.
They run through fresh operation-scoped sessions opened from
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
       __tablename__ = "app_queue_task"

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
can include the queue table in the application migration stream. The default
model is ``QueueTaskModel`` and uses the ``litestar_queue_task`` table. It is
imported when ``AdvancedAlchemyBackendConfig()`` is constructed, so app startup
normally puts it on Advanced Alchemy's base metadata before migration
autogenerate runs.

App-Owned Queue Model
~~~~~~~~~~~~~~~~~~~~~

Advanced Alchemy support includes a default queue model. Override it when the
application needs its own table name, base class, bind metadata, or Alembic
ownership. Compose ``QueueTaskModelMixin`` with an Advanced Alchemy base that
provides compatible ``id`` and ``created_at`` columns, then pass the resulting
model class to the backend:

.. code-block:: python

   from advanced_alchemy.base import UUIDAuditBase
   from advanced_alchemy.extensions.litestar import SQLAlchemyAsyncConfig

   from litestar_queues import QueueConfig
   from litestar_queues.backends.advanced_alchemy import AdvancedAlchemyBackendConfig, QueueTaskModelMixin

   class AppQueueTask(UUIDAuditBase, QueueTaskModelMixin):
       __tablename__ = "app_queue_task"

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

Advanced Alchemy Capability Notes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The Advanced Alchemy backend uses native SQLAlchemy and Advanced Alchemy
features where the dialect supports them:

.. list-table::
   :header-rows: 1

   * - Dialect family
     - Claim strategy
     - JSON storage
     - Keyed enqueue
   * - PostgreSQL
     - ``FOR UPDATE SKIP LOCKED`` candidate selection plus an ownership update.
     - ``JSONB`` through Advanced Alchemy's ``JsonB`` type.
     - Native ``ON CONFLICT`` upsert for keyed records.
   * - MySQL / MariaDB
     - ``FOR UPDATE SKIP LOCKED`` candidate selection plus an ownership update.
     - Native JSON through Advanced Alchemy's ``JsonB`` abstraction.
     - Native duplicate-key upsert for keyed records.
   * - Oracle
     - ``FOR UPDATE SKIP LOCKED`` without ``FETCH FIRST`` so Oracle can lock
       candidate rows correctly.
     - Oracle JSON/BLOB handling through Advanced Alchemy's ``JsonB`` type.
     - Native ``MERGE`` for keyed records.
   * - SQLite and other dialects
     - Optimistic compare-and-swap.
     - Native dialect JSON where SQLAlchemy provides it.
     - Portable key-check and insert fallback.

All paths keep the same public queue semantics: active keyed records deduplicate,
terminal keyed records can be replaced, stale recovery returns the affected task
IDs, and ownership-fence losses surface as normal queue lifecycle events.

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
       queue_backend=SQLSpecBackendConfig(config=...),
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
loads the queue configuration from ``LITESTAR_QUEUES_CONFIG_FACTORY`` before it
reads the task id, so custom ``env_prefix`` values are honored for variables
such as ``<PREFIX>_TASK_ID`` and ``<PREFIX>_TASK_MODULES``. It then loads
configured task modules, claims the persisted record, updates heartbeats,
executes the task through the shared queue service, and returns deterministic
process exit codes. A missing or invalid config factory exits with a distinct
configuration error instead of looking like a missing task record.

If the Cloud Run API call fails before a remote execution owns the task, the
backend logs and publishes a task event. By default
``fallback_execution_backend`` is ``None``, so the dispatch error surfaces and
the record remains routed to ``cloudrun``. To opt in to local recovery, set a
fallback explicitly, for example ``fallback_execution_backend="local"``. Status
checks that fail transiently are treated as still running so reconciliation does
not create false terminal states; a missing Cloud Run execution is treated as a
failed execution so eligible records can retry.

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
           config=sqlspec_config,
           create_schema=False,
           run_migrations=True,
           notifications=True,
           notification_channel="queue_notifications",
       ),
       execution_backend="local",
   )

PostgreSQL SQLSpec adapters can use SQLSpec's native ``listen_notify`` backend;
other adapters can use the durable ``table_queue`` backend. Oracle adapters can
use native ``aq`` or ``txeventq`` transports when the database user has AQ
privileges and the target queues are provisioned. These Oracle transports stay
explicit because queue provisioning is DBA-owned:

.. code-block:: python

   from sqlspec.adapters.oracledb import OracleAsyncConfig

   oracle_config = OracleAsyncConfig(
       connection_config={
           "host": "db.example.com",
           "port": 1521,
           "service_name": "FREEPDB1",
           "user": "queue_app",
           "password": "...",
           "min": 1,
           "max": 5,
       },
       extension_config={
           "events": {
               "backend": "txeventq",
               "aq_queue": "LQ_EVENTS_TXQ",
           }
       },
   )

   config = QueueConfig(
       queue_backend=SQLSpecBackendConfig(
           config=oracle_config,
           notifications=True,
           notify_transport="txeventq",
           event_settings={"aq_queue": "LQ_EVENTS_TXQ"},
       ),
       execution_backend="local",
   )

Queue notification channel names must be valid SQLSpec event identifiers.

SQLSpec Observability
~~~~~~~~~~~~~~~~~~~~~

The SQLSpec backend uses SQLSpec's ``ObservabilityRuntime`` for queue-domain
counters and spans. SQL statements executed by the backend already flow through
SQLSpec driver spans and statement observers, so query spans inherit SQLSpec
correlation context automatically.

Queue counters are recorded with these reserved names:

.. list-table::
   :header-rows: 1

   * - Metric
     - Meaning
   * - ``queue.enqueue``
     - Queue records inserted by ``enqueue`` or ``enqueue_many``.
   * - ``queue.claim``
     - Records successfully claimed for execution.
   * - ``queue.complete``
     - Records completed successfully.
   * - ``queue.fail``
     - Records moved to terminal failure by ``fail_task``.
   * - ``queue.retry``
     - Records requeued for another attempt.
   * - ``queue.stale_recovered``
     - Stale running records handled by stale recovery.
   * - ``queue.notify``
     - Worker wakeup notifications published through SQLSpec Events.
   * - ``queue.claim_lost``
     - Fenced completion or failure attempts rejected after ownership changed.
   * - ``queue.stale_failed``
     - Stale records moved to terminal failure.

Counters are available from SQLSpec diagnostics:

.. code-block:: python

   runtime = sqlspec_config.get_observability_runtime()
   queue_metrics = runtime.metrics_snapshot()

Set ``queue_observability=False`` on ``SQLSpecBackendConfig`` to disable only the
queue-domain counters and custom queue spans. SQLSpec driver query spans and
statement observers remain controlled by the SQLSpec config.

OpenTelemetry tracing and Prometheus statement metrics are enabled through
SQLSpec's optional extensions:

.. code-block:: python

   from sqlspec import SQLSpec
   from sqlspec.adapters.asyncpg import AsyncpgConfig
   from sqlspec.extensions.otel import enable_tracing
   from sqlspec.extensions.prometheus import enable_metrics

   from litestar_queues import QueueConfig
   from litestar_queues.backends.sqlspec import SQLSpecBackendConfig

   observability = enable_tracing(
       resource_attributes={"service.name": "queue-worker"},
   )
   observability = enable_metrics(base_config=observability)

   sqlspec = SQLSpec(observability_config=observability)
   sqlspec_config = AsyncpgConfig(
       pool_config={"dsn": "postgresql://queue@db/queues"},
   )
   sqlspec.add_config(sqlspec_config)

   config = QueueConfig(
       queue_backend=SQLSpecBackendConfig(
           sqlspec=sqlspec,
           config=sqlspec_config,
       ),
       execution_backend="local",
   )

Pool, connection, session, and query lifecycle hooks remain SQLSpec-owned. Attach
them to the same runtime when you need adapter-level lifecycle events:

.. code-block:: python

   def record_query_complete(context: dict[str, object]) -> None:
       ...

   runtime = sqlspec_config.get_observability_runtime()
   runtime.register_lifecycle_hook("on_query_complete", record_query_complete)

SQLSpec's Prometheus helper records bounded statement metrics such as
``sqlspec_driver_query_total`` and ``sqlspec_driver_query_duration_seconds``.
Applications that want the queue-domain counters in Prometheus can expose the
``metrics_snapshot()`` values through their own metrics bridge.

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
               "table_name": "app_queue_task",
               "notification_channel": "app_queue_notifications",
           }
       },
   )

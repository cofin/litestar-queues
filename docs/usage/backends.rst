Backends
========

Litestar Queues separates queue backends from execution backends.

Queue backends persist task state. The core package registers the ``memory``
backend for tests, local development, and in-process workers. The ``sqlspec``
backend is available when the SQLSpec extra is installed. Additional optional
extras are reserved for Advanced Alchemy, Redis, and Valkey integrations.

Execution backends decide where claimed tasks run. The core package registers
``immediate`` for inline execution and ``local`` for in-process worker
execution. A ``cloudrun`` extra is reserved for external execution.

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
``QueueConfig.queue_backend_config``:

.. code-block:: python

   from sqlspec.adapters.aiosqlite import AiosqliteConfig

   from litestar_queues import QueueConfig

   config = QueueConfig(
       queue_backend="sqlspec",
       queue_backend_config={
           "sqlspec_config": AiosqliteConfig(
               connection_config={"database": "queue.db"},
           ),
       },
       execution_backend="local",
   )

By default, the backend creates the queue table on startup. Set
``create_schema=False`` in ``queue_backend_config`` when schema management is
handled elsewhere. Applications that want SQLSpec to apply the packaged queue
migration can set ``run_migrations=True``:

.. code-block:: python

   config = QueueConfig(
       queue_backend="sqlspec",
       queue_backend_config={
           "sqlspec_config": AiosqliteConfig(
               connection_config={"database": "queue.db"},
           ),
           "create_schema": False,
           "run_migrations": True,
       },
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

   sqlspec = SQLSpec()
   sqlspec_config = AiosqliteConfig(connection_config={"database": "queue.db"})
   sqlspec.add_config(sqlspec_config)

   app = Litestar(
       plugins=[
           SQLSpecPlugin(sqlspec),
           QueuePlugin(
               QueueConfig(
                   queue_backend="sqlspec",
                   queue_backend_config={
                       "sqlspec": sqlspec,
                       "sqlspec_config": sqlspec_config,
                   },
                   execution_backend="local",
               )
           )
       ],
   )

SQLSpec persists task arguments, keyword arguments, metadata, and results using
SQLSpec's serializer. Packaged migrations are registered with SQLSpec's
extension runner as ``ext_litestar_queues_0001``. Applications with their own
migration flow can set both ``create_schema=False`` and ``run_migrations=False``.

Backends
========

Litestar Queues separates storage backends from execution backends.

Storage backends persist task state. The core package registers the ``memory``
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

The core package import does not require optional storage or execution client
libraries.

SQLSpec
-------

Install the SQLSpec extra when a queue needs SQL-backed persistence:

.. code-block:: bash

   pip install litestar-queues[sqlspec]

Configure SQLSpec storage by passing a SQLSpec adapter config through
``QueueConfig.storage_backend_config``:

.. code-block:: python

   from sqlspec.adapters.aiosqlite import AiosqliteConfig

   from litestar_queues import QueueConfig

   config = QueueConfig(
       storage_backend="sqlspec",
       storage_backend_config={
           "sqlspec_config": AiosqliteConfig(
               connection_config={"database": "queue.db"},
           ),
       },
       execution_backend="local",
   )

By default, the backend creates the queue table on startup. Set
``create_schema=False`` in ``storage_backend_config`` when schema management is
handled elsewhere. Applications that want SQLSpec to apply the packaged queue
migration can set ``run_migrations=True``:

.. code-block:: python

   config = QueueConfig(
       storage_backend="sqlspec",
       storage_backend_config={
           "sqlspec_config": AiosqliteConfig(
               connection_config={"database": "queue.db"},
           ),
           "create_schema": False,
           "run_migrations": True,
       },
       execution_backend="local",
   )

Litestar applications can also let the queue backend register SQLSpec's
first-party plugin during app initialization:

.. code-block:: python

   from litestar import Litestar
   from sqlspec.adapters.aiosqlite import AiosqliteConfig

   from litestar_queues import QueueConfig, QueuePlugin

   app = Litestar(
       plugins=[
           QueuePlugin(
               QueueConfig(
                   storage_backend="sqlspec",
                   storage_backend_config={
                       "sqlspec_config": AiosqliteConfig(
                           connection_config={"database": "queue.db"},
                       ),
                       "register_plugin": True,
                   },
                   execution_backend="local",
               )
           )
       ],
   )

SQLSpec storage persists JSON-compatible task arguments, keyword arguments,
metadata, and results. Applications with their own migration flow can set both
``create_schema=False`` and ``run_migrations=False``.

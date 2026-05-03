Testing
-------

The memory storage backend is the default scaffold backend for tests and local
development.

.. code-block:: python

   from litestar_queues import QueueConfig

   config = QueueConfig(storage_backend="memory", execution_backend="immediate")

Use ``QueueConfig.provide_service()`` when a test needs the same lifecycle shape
as Litestar dependency injection:

.. code-block:: python

   async with config.provide_service() as queue_service:
       assert queue_service.config is config

Runtime assertions for task persistence and result handling belong with the
backend contract tests added in later implementation chapters.

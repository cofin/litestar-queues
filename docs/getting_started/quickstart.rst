==========
Quickstart
==========

Register a task with :func:`litestar_queues.task`:

.. code-block:: python

   from litestar_queues import task


   @task("accounts.sync", queue="accounts", retries=3, timeout=300)
   async def sync_account(account_id: str) -> dict[str, str]:
       return {"account_id": account_id, "status": "synced"}

Attach the queue plugin to a Litestar application:

.. code-block:: python

   from litestar import Litestar, post
   from litestar.di import NamedDependency
   from litestar_queues import QueueConfig, QueuePlugin, QueueService


   @post("/accounts/{account_id:str}/sync")
   async def create_task(account_id: str, queue_service: NamedDependency[QueueService]) -> dict[str, str]:
       result = await queue_service.enqueue(sync_account, account_id)
       return {"task_id": str(result.id), "status": result.status or "queued"}


   app = Litestar(route_handlers=[create_task], plugins=[QueuePlugin(config=QueueConfig())])

The default configuration runs a worker inside the Litestar application process.
That keeps local and lightweight deployments simple. For heavier deployments,
use a shared queue backend, disable the in-app worker in the web process, and run
workers separately:

.. code-block:: python

   config = QueueConfig(in_app_worker=False)

.. code-block:: console

   $ LITESTAR_APP=app.asgi:app litestar queues run --drain-timeout 30

Next Steps
==========

* Configure runtime settings in :doc:`../usage/configuration`.
* Add retries, keys, metadata, and execution overrides in
  :doc:`../usage/tasks`.
* Choose a persistent backend in :doc:`../usage/backends`.

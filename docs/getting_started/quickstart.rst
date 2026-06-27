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


   config = QueueConfig(
       queue_backend="memory",
       execution_backend="local",
       start_worker=True,
   )


   @post("/accounts/{account_id:str}/sync")
   async def create_task(account_id: str, queue_service: NamedDependency[QueueService]) -> dict[str, str]:
       result = await queue_service.enqueue(sync_account, account_id)
       return {"task_id": str(result.id), "status": result.status or "queued"}


   app = Litestar(route_handlers=[create_task], plugins=[QueuePlugin(config=config)])

For scripts and tests that do not need an app worker, use a standalone service:

.. code-block:: python

   from litestar_queues import QueueConfig, QueueService


   async with QueueService(QueueConfig(execution_backend="immediate")) as queue_service:
       result = await queue_service.enqueue(sync_account, "acct-123")
       assert result.status == "completed"
       assert result.result == {"account_id": "acct-123", "status": "synced"}

The task wrapper also exposes ``enqueue()`` with the default immediate
in-memory service:

.. code-block:: python

   result = await sync_account.enqueue("acct-123")
   await result.refresh()

Next Steps
==========

* Configure runtime settings in :doc:`../usage/configuration`.
* Add retries, keys, metadata, and execution overrides in
  :doc:`../usage/tasks`.
* Choose a persistent backend in :doc:`../usage/backends`.

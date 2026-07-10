==========
Quickstart
==========

This first application stores work in memory and runs it with the worker that
starts inside the Litestar process. You can replace both choices later without
changing the task function or route.

Install Litestar Queues
=======================

.. code-block:: bash

   pip install litestar-queues

Create ``app.py``
=================

Copy this complete file:

.. code-block:: python

   from litestar import Litestar, post
   from litestar.di import NamedDependency

   from litestar_queues import QueueConfig, QueuePlugin, QueueService, task


   @task("accounts.sync", queue="accounts", timeout=30)
   async def sync_account(account_id: str) -> dict[str, str]:
       return {"account_id": account_id, "status": "synced"}


   @post("/accounts/{account_id:str}/sync")
   async def create_sync_job(
       account_id: str,
       queue_service: NamedDependency[QueueService],
   ) -> dict[str, str]:
       result = await queue_service.enqueue(sync_account, account_id)
       return {"task_id": str(result.id), "status": result.status or "pending"}


   app = Litestar(
       route_handlers=[create_sync_job],
       plugins=[QueuePlugin(config=QueueConfig())],
   )

Run and verify it
=================

Start Litestar:

.. code-block:: bash

   LITESTAR_APP=app:app litestar run --reload

In another terminal, enqueue the task:

.. code-block:: bash

   curl -X POST http://127.0.0.1:8000/accounts/acct-123/sync

You receive JSON shaped like this:

.. code-block:: json

   {"task_id":"...","status":"pending"}

The ID identifies the saved task record. ``pending`` is its status when it
enters the queue. The in-app worker may complete the task immediately after
the response is created.

Next steps
==========

* :doc:`../usage/concepts` explains records, workers, and backend choices.
* :doc:`../usage/results` shows how to refresh and wait for a result.
* :doc:`../usage/workers` moves work into standalone worker processes.
* :doc:`../usage/backends` replaces process-local storage for production.
* :doc:`../usage/events` publishes progress for applications and operators.

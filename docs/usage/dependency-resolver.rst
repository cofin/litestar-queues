===================
Dependency Resolver
===================

``QueueConfig.task_dependency_resolver`` lets queued tasks receive services
from an external dependency-injection (DI) container. A DI container creates
and supplies objects such as database sessions, settings, and HTTP clients.
Use this hook with Dishka, an in-house container, or a simple custom provider
until Litestar 3.0 beta DI becomes the standard integration.

Litestar Queues awaits the resolver once for each task attempt. This happens
after the ``task.started`` event and before the task function runs. It adds the
returned mapping to the task's keyword arguments. The package-owned
``_job_id`` and ``_task_context`` values always override resolver output.

Type Alias
==========

.. code-block:: python

   from litestar_queues import TaskDependencyResolver

``TaskDependencyResolver`` is exported from the package root and is also
available through :attr:`litestar_queues.QueueConfig.signature_namespace`.
It names an async function with this signature::

   async def resolver(task, record, task_context) -> dict[str, Any]: ...

* ``task`` - the registered :class:`litestar_queues.Task` wrapper.
* ``record`` - the :class:`litestar_queues.QueuedTaskRecord` that is about to
  run.
* ``task_context`` - the active
  :class:`litestar_queues.TaskExecutionContext` (the same instance bound to the
  context variable for the duration of the attempt).

Wiring an External Container
============================

This example uses a small custom container so the pattern works with any DI
framework. Replace ``Container.get(...)`` with the equivalent method from your
container. The resolver only needs to return keyword arguments that match the
task function's parameters.

.. code-block:: python

   from typing import Any

   from litestar_queues import (
       QueueConfig,
       QueueService,
       Task,
       TaskExecutionContext,
       task,
   )
   from litestar_queues.models import QueuedTaskRecord


   class Container:
       """Minimal stand-in for whichever DI container an adopter already runs."""

       def __init__(self) -> None:
           self._services: dict[str, Any] = {}

       def register(self, key: str, service: Any) -> None:
           self._services[key] = service

       def get(self, key: str) -> Any:
           return self._services[key]


   container = Container()
   container.register("settings", {"environment": "production"})


   async def resolve_dependencies(
       _task: Task[Any, Any],
       _record: QueuedTaskRecord,
       _context: TaskExecutionContext,
   ) -> dict[str, Any]:
       return {"settings": container.get("settings")}


   @task("reports.generate")
   async def generate_report(*, settings: dict[str, Any]) -> str:
       return f"generated for {settings['environment']}"


   async def main() -> None:
       config = QueueConfig(task_dependency_resolver=resolve_dependencies)
       async with QueueService(config) as service:
           result = await service.enqueue("reports.generate")
           await result.refresh()
           assert result.result == "generated for production"

Resolver Failures and Retries
=============================

Resolver errors follow the normal task failure path. Litestar Queues records
the exception through ``fail_task``, counts it against ``record.max_retries``,
and emits ``task.failed``. Resolver failures do not have a separate retry
limit. Handle connection errors in the same way as errors from the task body.

Per-Attempt Invocation
======================

The resolver runs once per attempt. Return fresh per-attempt resources. Do not
reuse a session, transaction, or cached object from a failed attempt because it
may contain invalid state. You may create an expensive shared resource, such as
a connection pool, once per process. The resolver should still return a fresh
handle from that resource for each attempt.

See Also
========

* :doc:`configuration` for the rest of the ``QueueConfig`` surface
* :doc:`tasks` for task registration and the ``_job_id`` / ``_task_context``
  sentinels
* :doc:`events` for the lifecycle events that bracket resolver execution

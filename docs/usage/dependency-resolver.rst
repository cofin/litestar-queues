===================
Dependency Resolver
===================

The ``QueueConfig.task_dependency_resolver`` hook lets callers wire an external
dependency container into queued task callables. Until Litestar 3.0 beta DI
becomes the canonical answer, this is the bridge that lets adopters with their
own DI machinery (Dishka, an in-house container, or a hand-rolled provider)
inject services such as database sessions, settings, or HTTP clients into task
functions.

When configured, the resolver is awaited exactly once per task execution
attempt, after the ``task.started`` lifecycle event and before the registered
task callable runs. The returned mapping is merged into the task's keyword
arguments. The package-owned sentinels ``_job_id`` and ``_task_context`` always
take precedence over resolver output.

Type Alias
==========

.. code-block:: python

   from litestar_queues import TaskDependencyResolver

``TaskDependencyResolver`` is exported from the package root and is also
available through :attr:`litestar_queues.QueueConfig.signature_namespace`. It
is an alias for an ``async`` callable with the signature::

   async def resolver(task, record, task_context) -> dict[str, Any]: ...

* ``task`` - the registered :class:`litestar_queues.Task` wrapper
* ``record`` - the :class:`litestar_queues.QueuedTaskRecord` that is about to
  execute
* ``task_context`` - the active
  :class:`litestar_queues.TaskExecutionContext` (the same instance bound to the
  contextvar for the duration of the attempt)

Wiring an External Container
============================

The example below uses a tiny home-grown container so the pattern is portable
across DI frameworks. Replace ``Container.get(...)`` with whatever your
container exposes - the resolver only has to return a mapping of kwargs that
match parameters declared on the task callable.

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

The resolver participates in the standard task failure path. If it raises, the
exception is recorded through ``fail_task``, counts toward
``record.max_retries``, and emits a ``task.failed`` lifecycle event. There is
no separate retry budget for resolver failures. A resolver that connects to an
external system should treat connection errors the same way the task body
does.

Per-Attempt Invocation
======================

Resolvers are called once per attempt. They should treat each invocation as a
fresh attempt and avoid memoizing per-record state across retries. Re-using a
session, transaction, or cached object that survived a previous failed attempt
is an easy way to leak corrupted state into a retry. If a resource is
expensive to build, build it once at process scope (a module-level singleton or
a shared connection pool) and let the resolver hand out fresh handles per
attempt.

See Also
========

* :doc:`configuration` for the rest of the ``QueueConfig`` surface
* :doc:`tasks` for task registration and the ``_job_id`` / ``_task_context``
  sentinels
* :doc:`events` for the lifecycle events that bracket resolver execution

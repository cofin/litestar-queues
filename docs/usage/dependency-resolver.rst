=================
Task Dependencies
=================

``QueueConfig.task_dependency_resolver`` and ``QueueConfig.task_dependency_provider`` let queued tasks receive services from an external dependency-injection (DI) container. A DI container creates and supplies objects such as database sessions, settings, and HTTP clients.

Choosing a Hook
===============

* **Resolver** (``task_dependency_resolver``): Stateless keyword arguments, no cleanup edge.
* **Provider** (``task_dependency_provider``): The attempt owns a resource that must be released.

Configuring both hooks at the same time raises a :exc:`QueueConfigurationError`.

Stateless Resolver
==================

Litestar Queues awaits the resolver once for each task attempt. This happens after the ``task.started`` event and before the task function runs. It adds the returned mapping to the task's keyword arguments. The package-owned ``_task_context`` value always overrides resolver output.

Type Alias
----------

.. code-block:: python

   from litestar_queues import TaskDependencyResolver

``TaskDependencyResolver`` is exported from the package root. It names an async function with this signature::

   async def resolver(task, record, task_context) -> dict[str, Any]: ...

* ``task`` - the registered :class:`litestar_queues.Task` wrapper.
* ``record`` - the :class:`litestar_queues.QueuedTaskRecord` that is about to run.
* ``task_context`` - the active :class:`litestar_queues.TaskExecutionContext` (the same instance bound to the context variable for the duration of the attempt).

Wiring an External Container
----------------------------

This example uses a small custom container so the pattern works with any DI framework. Replace ``Container.get(...)`` with the equivalent method from your container. The resolver only needs to return keyword arguments that match the task function's parameters.

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
-----------------------------

Resolver errors follow the normal task failure path. Litestar Queues records the exception through ``fail_task``, counts it against ``record.max_retries``, and emits ``task.failed``. Resolver failures do not have a separate retry limit. Handle connection errors in the same way as errors from the task body.

Per-Attempt Invocation
----------------------

The resolver runs once per attempt. Return fresh per-attempt resources. Do not reuse a session, transaction, or cached object from a failed attempt because it may contain invalid state. You may create an expensive shared resource, such as a connection pool, once per process. The resolver should still return a fresh handle from that resource for each attempt.

Attempt-scoped Provider
=======================

The ``task_dependency_provider`` hook manages resources that require deterministic cleanup, like database transactions or network connections.

It is an async context manager with the following signature::

   @contextlib.asynccontextmanager
   async def provider(task, record, task_context) -> AsyncIterator[dict[str, Any]]: ...

The provider yields a dictionary of keyword arguments to inject into the task function.

.. code-block:: python

   import contextlib
   from typing import Any, AsyncIterator

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

       @contextlib.asynccontextmanager
       async def scoped(self) -> AsyncIterator[Any]:
           yield {"connection": "active"}
           # Cleanup happens here


   container = Container()


   @contextlib.asynccontextmanager
   async def provide_dependencies(
       _task: Task[Any, Any],
       _record: QueuedTaskRecord,
       _context: TaskExecutionContext,
   ) -> AsyncIterator[dict[str, Any]]:
       async with container.scoped() as scope:
           yield {"db": scope["connection"]}


   @task("db.update")
   async def update_db(*, db: str) -> str:
       return f"updated with {db}"


   async def main() -> None:
       config = QueueConfig(task_dependency_provider=provide_dependencies)
       async with QueueService(config) as service:
           result = await service.enqueue("db.update")
           await result.refresh()
           assert result.result == "updated with active"

The guarantee: the scope is entered at most once and ``__aexit__`` is awaited exactly once, for success, retryable failure, terminal failure, timeout, cooperative cancellation, durable cancellation, claim loss, and shutdown interruption.

Acquisition runs inside the attempt timeout, so a slow provider fails the attempt and consumes retry budget.

``__aexit__`` sees ``asyncio.CancelledError`` for timeout, cancellation, claim loss, and shutdown alike — read ``record`` or ``task_context`` to understand the outcome, never the exception type.

A cleanup failure after a successful body fails the attempt, while a cleanup failure after any other outcome is logged and dropped so it cannot hide the real reason.

A truthy ``__aexit__`` return is ignored — the provider is a resource scope, not an exception filter.

Cleanup is awaited exactly once, but at shutdown it is bounded by ``final_cancel_timeout`` (and the hard-exit watchdog), so a provider must not rely on unbounded cleanup for correctness.

Provider Objects and Process Lifecycle
======================================

A provider object that exposes async or sync ``open()`` and ``close()`` methods joins the ``QueueService`` lifecycle. The service calls ``open()`` before opening the queue backend and calls ``close()`` after tearing down every other service resource. This holds in every placement (ASGI, server, external worker, one-shot consumer) and includes partial-open rollback.

Note that the server-worker child re-imports the application through ``QUEUES_CONFIG_FACTORY`` and builds its own provider instance. A provider must be constructible in a fresh process and must not assume shared in-process state with the parent.

See Also
========

* :doc:`configuration` for the rest of the ``QueueConfig`` surface
* :doc:`tasks` for task registration and the typed ``_task_context`` value
* :doc:`events` for the lifecycle events that bracket resolver execution

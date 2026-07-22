==================
Execution Backends
==================

Registry
========

Execution backends are registered through ``litestar_queues.execution``.

.. autofunction:: litestar_queues.execution.get_execution_backend

.. autofunction:: litestar_queues.execution.get_execution_backend_class

.. autofunction:: litestar_queues.execution.execution_backend

.. autofunction:: litestar_queues.execution.list_execution_backends

Base Backend
============

.. automodule:: litestar_queues.execution.base
   :members:
   :undoc-members:
   :show-inheritance:

Immediate
=========

.. automodule:: litestar_queues.execution.immediate
   :members:
   :undoc-members:
   :show-inheritance:

Local
=====

.. automodule:: litestar_queues.execution.local
   :members:
   :undoc-members:
   :show-inheritance:

Cloud Run
=========

.. automodule:: litestar_queues.execution.cloudrun.config
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: litestar_queues.execution.cloudrun.backend
   :members:
   :undoc-members:
   :show-inheritance:

Consumer
========

The programmatic entry point for an external executor: run one queued record by
id and exit with a deterministic code. This is the in-process twin of
``litestar queues run-task`` -- use it from a serverless handler or a custom
runner that cannot shell out. The live queue record stays authoritative; the
consumer re-fetches it by id and fences on the retry count at claim time.

.. automodule:: litestar_queues.consumer
   :members:
   :undoc-members:
   :show-inheritance:

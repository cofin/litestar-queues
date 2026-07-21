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

Dispatch Envelope
=================

The universal, versioned routing slip carried to every external execution
backend. External consumers decode it and re-fetch the live queue record.

.. automodule:: litestar_queues.execution.envelope
   :members:
   :undoc-members:
   :show-inheritance:

API Reference
=============

The main public API is exported through the ``litestar_queues`` module.

Configuration
-------------

.. automodule:: litestar_queues.config
   :members:
   :undoc-members:
   :show-inheritance:

Service
-------

.. automodule:: litestar_queues.service
   :members:
   :undoc-members:
   :show-inheritance:

Plugin
------

.. automodule:: litestar_queues.plugin
   :members:
   :undoc-members:
   :show-inheritance:

Exceptions
----------

.. automodule:: litestar_queues.exceptions
   :members:
   :undoc-members:
   :show-inheritance:

Storage Backends
----------------

Storage backends are available via ``litestar_queues.backends``.

.. autofunction:: litestar_queues.backends.get_storage_backend

.. autofunction:: litestar_queues.backends.get_storage_backend_class

.. autofunction:: litestar_queues.backends.storage_backend

.. autofunction:: litestar_queues.backends.list_storage_backends

.. automodule:: litestar_queues.backends.base
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: litestar_queues.backends.memory
   :members:
   :undoc-members:
   :show-inheritance:

Execution Backends
------------------

Execution backends are available via ``litestar_queues.execution``.

.. autofunction:: litestar_queues.execution.get_execution_backend

.. autofunction:: litestar_queues.execution.get_execution_backend_class

.. autofunction:: litestar_queues.execution.execution_backend

.. autofunction:: litestar_queues.execution.list_execution_backends

.. automodule:: litestar_queues.execution.base
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: litestar_queues.execution.immediate
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: litestar_queues.execution.local
   :members:
   :undoc-members:
   :show-inheritance:

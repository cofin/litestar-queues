==============
Queue Backends
==============

Registry
========

Queue backends are registered through ``litestar_queues.backends``.

.. autofunction:: litestar_queues.backends.get_queue_backend

.. autofunction:: litestar_queues.backends.get_queue_backend_class

.. autofunction:: litestar_queues.backends.queue_backend

.. autofunction:: litestar_queues.backends.list_queue_backends

Base Backend
============

.. automodule:: litestar_queues.backends.base
   :members:
   :undoc-members:
   :show-inheritance:

Memory
======

.. automodule:: litestar_queues.backends.memory.backend
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: litestar_queues.backends.memory.event_log
   :members:
   :undoc-members:
   :show-inheritance:

SQLSpec
=======

.. automodule:: litestar_queues.backends.sqlspec.backend
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: litestar_queues.backends.sqlspec.config
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: litestar_queues.backends.sqlspec.schema
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: litestar_queues.backends.sqlspec.stores.factory
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: litestar_queues.backends.sqlspec.event_log
   :members:
   :undoc-members:
   :show-inheritance:

Advanced Alchemy
================

.. automodule:: litestar_queues.backends.advanced_alchemy.backend
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: litestar_queues.backends.advanced_alchemy.config
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: litestar_queues.backends.advanced_alchemy.mixins
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: litestar_queues.backends.advanced_alchemy.repository
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: litestar_queues.backends.advanced_alchemy.service
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: litestar_queues.backends.advanced_alchemy.event_log
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: litestar_queues.backends.advanced_alchemy.models
   :members:
   :undoc-members:
   :show-inheritance:

Redis
=====

.. automodule:: litestar_queues.backends.redis.backend
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: litestar_queues.backends.redis.config
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: litestar_queues.backends.redis.event_log
   :members:
   :undoc-members:
   :show-inheritance:

Valkey
======

.. automodule:: litestar_queues.backends.valkey.backend
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: litestar_queues.backends.valkey.config
   :members:
   :undoc-members:
   :show-inheritance:

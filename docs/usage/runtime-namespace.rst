=================
Runtime namespace
=================

Every runtime name the package owns — loggers, metrics, channels, cache keys,
process names, generated routes — is derived from one setting. Most
applications never change it. Set it when package-owned names have to carry
your own branding, or when two queue plugins run in the same process and their
names must not collide.

Set ``namespace`` once on :class:`~litestar_queues.QueueConfig`:

.. code-block:: python

   from litestar_queues import QueueConfig

   queue_config = QueueConfig(namespace="myapp")

That one value produces names such as ``myapp.wakeups`` for telemetry,
``myapp.worker`` for loggers, ``myapp:worker_wakeups`` for channels and Redis or
Valkey keys, ``myapp_service`` for Litestar state and dependency registration,
``MYAPP_SERVER_NONCE`` for private process coordination, and ``myapp-worker`` for
process, thread, and temporary-resource names. Default event streams mount
under ``/myapp/events`` and Cloud Tasks delivery resources begin with ``myapp-``.
Multiple queue plugins can use different namespaces without mutating
process-global naming state.

What it does not rename
=======================

Explicit component settings still win. User-authored task and queue names are
never rewritten. SQL table names and Advanced Alchemy model classes also remain
under their existing backend settings; ``namespace`` does not derive them.

Bootstrap environment variables
===============================

External one-task executors use two stable bootstrap variables:
``QUEUES_CONFIG_FACTORY`` and ``QUEUES_TASK_ID``. They are intentionally not
namespace-derived because the consumer must locate the config factory before it
can load ``QueueConfig.namespace``. After the factory is loaded, other derived
runtime names use that config's namespace.

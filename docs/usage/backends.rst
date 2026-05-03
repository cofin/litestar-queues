Backends
========

Litestar Queues separates storage backends from execution backends.

Storage backends persist task state. The core package registers the ``memory``
backend for tests, local development, and in-process workers. Optional extras
are reserved for SQLSpec, Advanced Alchemy, Redis, and Valkey integrations.

Execution backends decide where claimed tasks run. The core package registers
``immediate`` for inline execution and ``local`` for in-process worker
execution. A ``cloudrun`` extra is reserved for external execution.

Install optional extras only when an application needs them:

.. code-block:: bash

   pip install litestar-queues[sqlspec]
   pip install litestar-queues[advanced-alchemy]
   pip install litestar-queues[redis]
   pip install litestar-queues[valkey]
   pip install litestar-queues[cloudrun]

The core package import does not require optional storage or execution client
libraries.

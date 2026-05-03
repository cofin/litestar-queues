Backends
========

Litestar Queues separates storage backends from execution backends.

Storage backends persist task state. The scaffold registers the ``memory``
backend and reserves optional extras for SQLSpec, Advanced Alchemy, Redis, and
Valkey integrations.

Execution backends decide where claimed tasks run. The scaffold registers
``immediate`` and ``local`` placeholders and reserves a ``cloudrun`` extra for
external execution.

Install optional extras only when an application needs them:

.. code-block:: bash

   pip install litestar-queues[sqlspec]
   pip install litestar-queues[advanced-alchemy]
   pip install litestar-queues[redis]
   pip install litestar-queues[valkey]
   pip install litestar-queues[cloudrun]

The core package import does not require optional storage or execution client
libraries.

==========================
Command-Line Interface
==========================

``QueuePlugin`` implements Litestar's :class:`~litestar.plugins.CLIPluginProtocol`
and contributes a ``queues`` subcommand group to the ``litestar`` CLI. Three
subcommands ship today: ``run``, ``status``, and ``scheduler-health``. A
``discover_tasks`` helper supports adopters with ``app.domain.<x>.jobs/``
layouts.

Pre-requisites
==============

Every ``litestar queues …`` invocation resolves the application the same way
``litestar run`` and ``litestar routes`` do: via ``LITESTAR_APP``, the
``--app`` flag, or one of the standard discovery paths (``app.py``,
``asgi.py``, ``application.py``, ``app/__init__.py``). When none are
present, the CLI errors before any queue subcommand runs.

``click`` is not a runtime dependency of ``litestar-queues``. It arrives
transitively through ``litestar``; importing ``litestar_queues`` (without
invoking the CLI) does **not** load ``click`` into ``sys.modules``.

``litestar queues run``
=======================

Starts a standalone worker fleet outside the Litestar web app process. Use
this for sidecar worker containers, ``systemd`` units, or Cloud Run jobs.

.. code-block:: console

   $ LITESTAR_APP=app.asgi:app litestar queues run --drain-timeout 30

Options:

* ``--queue NAME`` (repeatable) — only claims records from the named queues.
  When omitted, the worker uses :attr:`QueueConfig.worker_queues`; when both
  are empty, it claims from every queue.
* ``--max-concurrency N`` — overrides
  :attr:`QueueConfig.worker_max_concurrency` for this run.
* ``--drain-timeout SECONDS`` — wait time after ``SIGTERM``/``SIGINT``
  before escalating to cancellation. Defaults to
  :attr:`QueueConfig.worker_graceful_shutdown_timeout` (30s).

Signal handling
---------------

``SIGTERM`` and ``SIGINT`` both trigger a graceful drain:
``Worker.stop()`` is set and the worker task is awaited up to
``--drain-timeout``. A **second** signal during drain escalates: every
running asyncio task is cancelled immediately. Exit codes:

============  ==================================================================
Code          Meaning
============  ==================================================================
``0``         Clean drain.
``1``         Worker raised an unexpected exception.
``2``         Drain exceeded the timeout and tasks were cancelled.
============  ==================================================================

``SIGTERM`` requires a POSIX host. Windows only meaningfully delivers
``SIGINT`` (Ctrl+C); ``add_signal_handler`` raises ``NotImplementedError``
there and the CLI falls back to ``signal.signal``.

``litestar queues status``
==========================

Prints queue status counts.

.. code-block:: console

   $ litestar queues status
   Status         Count
   ------------  ------
   pending            3
   scheduled          0
   running            1
   completed        120
   failed             2
   cancelled          0
   total            126

Options:

* ``--queue NAME`` (advisory) — backend-side filtering is not yet
  enforced; flag is logged to ``stderr``.
* ``--json`` — emit a single JSON object with the seven keys
  (``pending``, ``scheduled``, ``running``, ``completed``, ``failed``,
  ``cancelled``, ``total``). Output uses
  ``sqlspec.utils.serializers.to_json`` when available for symmetry with
  the camelCase wire format; falls back to stdlib ``json``.

Exit codes: ``0`` on success, ``1`` on backend error.

``litestar queues scheduler-health``
====================================

Exits non-zero when no completion record exists for a configured canary
task within the staleness window.

.. code-block:: console

   $ litestar queues scheduler-health --minutes 5
   healthy: scheduler.heartbeat completed 2026-05-13 21:17:51.116337+00:00

The package does **not** auto-register a canary task. Operators register
a recurring no-op task whose name matches
:attr:`QueueConfig.scheduler_canary_task` (default
``"scheduler.heartbeat"``):

.. code-block:: python

   from litestar_queues import task


   @task("scheduler.heartbeat", interval=60)
   async def heartbeat() -> None:
       return None

Exit codes:

============  ==================================================================
Code          Meaning
============  ==================================================================
``0``         Healthy — canary completed within the window.
``3``         Canary task is not registered. Register a recurring task
              with the configured name or set
              ``QueueConfig.scheduler_canary_task`` accordingly.
``4``         Stale — no completion record found within ``--minutes``.
============  ==================================================================

``--minutes`` defaults to 5. ``--json`` is not in scope for this
subcommand.

``discover_tasks``
==================

Adopters with an ``app.domain.<x>.jobs/`` layout can use the
:func:`~litestar_queues.discover_tasks` walker to import every module
under any ``jobs`` subpackage at startup, replacing manual enumeration
in :attr:`QueueConfig.task_modules`:

.. code-block:: python

   from litestar import Litestar

   from litestar_queues import QueueConfig, QueuePlugin, discover_tasks


   def create_app() -> Litestar:
       discover_tasks("app.domain")
       return Litestar(plugins=[QueuePlugin(QueueConfig(queue_backend="memory"))])

Signature:

.. autofunction:: litestar_queues.discover_tasks
   :noindex:

The default ``subpackage="jobs"`` matches modules whose dotted path
contains a ``jobs`` segment (e.g. ``app.domain.billing.jobs.send_invoice``).
The return value is a sorted, deduplicated tuple of fully-qualified task
names that reflects the post-walk registry, suitable for logging or
metrics labels.

Deployment example
==================

For a sidecar worker fleet behind Granian-served web pods (e.g. Cloud
Run service + Cloud Run job, or Kubernetes Deployment + StatefulSet):

.. code-block:: yaml

   # worker.service (systemd unit, abridged)
   [Service]
   Environment=LITESTAR_APP=app.asgi:app
   ExecStart=/usr/local/bin/litestar queues run --drain-timeout 60
   Restart=on-failure
   TimeoutStopSec=90

The ``TimeoutStopSec`` value should exceed ``--drain-timeout`` so the
service manager does not ``SIGKILL`` the worker while it is still
draining.

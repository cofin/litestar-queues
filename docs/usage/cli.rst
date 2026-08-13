==========================
Command-Line Interface
==========================

``QueuePlugin`` implements Litestar's :class:`~litestar.plugins.CLIPluginProtocol`
and adds a ``queues`` group to the ``litestar`` CLI. It provides ``run``,
``run-consumer``, ``status``, ``scheduler-health``, ``run-task``, and
``run-maintenance``. ``run-task`` is the one-record external-executor command
described in :doc:`deployment/cloud-run`. The ``discover_tasks`` helper supports
applications that keep tasks under ``app.domain.<x>.jobs/``.

Pre-requisites
==============

Every ``litestar queues …`` command finds the application the same way
``litestar run`` and ``litestar routes`` do: via ``LITESTAR_APP``, the
``--app`` flag, or one of the standard discovery paths (``app.py``,
``asgi.py``, ``application.py``, ``app/__init__.py``). When none are
present, the CLI errors before any queue subcommand runs.

``click`` is not a direct runtime dependency of ``litestar-queues``. Litestar
installs it. Importing ``litestar_queues`` without using the CLI does **not**
load ``click`` into ``sys.modules``.

``litestar queues run``
=======================

Starts standalone workers outside the Litestar web process. Use this command
for sidecar worker containers, ``systemd`` units, or Cloud Run jobs.

.. code-block:: console

   $ LITESTAR_APP=app.asgi:app litestar queues run --drain-timeout 30

Options:

* ``--queue NAME`` (repeatable) — only claims records from the named queues.
  When omitted, the worker uses :attr:`~litestar_queues.WorkerConfig.queues`; when both
  are empty, it claims from every queue.
* ``--max-concurrency N`` — overrides
  :attr:`~litestar_queues.WorkerConfig.max_concurrency` for this run.
* ``--drain-timeout SECONDS`` — wait time after ``SIGTERM``/``SIGINT``
  before escalating to cancellation. Defaults to
  :attr:`~litestar_queues.WorkerConfig.graceful_shutdown_timeout` (30s).

Signal handling
---------------

``SIGTERM`` and ``SIGINT`` both start a graceful shutdown. The worker stops
claiming new tasks and waits up to ``--drain-timeout`` for running tasks. A
**second** signal cancels every running asyncio task immediately. Exit codes:

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

``litestar queues run-consumer``
================================

Starts a long-running consumer that pulls task identifiers from an external
broker and executes them. Use it when ``execution_backend`` is RabbitMQ, SQS, or
Pub/Sub — the broker delivers the work instead of a worker polling the queue
store. See :doc:`deployment/rabbitmq`, :doc:`deployment/sqs`, and
:doc:`deployment/pubsub` for the surrounding deployment.

.. code-block:: console

   $ LITESTAR_APP=app.asgi:app litestar queues run-consumer --backend rabbitmq

Options:

* ``--backend [pubsub|rabbitmq|sqs]`` — **required**. Must name the same backend
  the application already configures; a mismatch is refused rather than silently
  consuming from somewhere else.
* ``--max-concurrency N`` — how many deliveries run at once. Defaults to
  :attr:`~litestar_queues.WorkerConfig.max_concurrency`.
* ``--drain-timeout SECONDS`` — wait time after ``SIGTERM``/``SIGINT`` before
  in-flight deliveries are cancelled. Defaults to
  :attr:`~litestar_queues.WorkerConfig.graceful_shutdown_timeout`.

Signals behave as they do for ``run``: the first stops consuming and drains, a
second cancels immediately, and a third exits the process without waiting.

Exit codes:

============  ==================================================================
Code          Meaning
============  ==================================================================
``0``         Clean drain.
``1``         Configuration error — no application found, an ephemeral or
              in-memory queue backend, ``--backend`` disagreeing with the
              configured execution backend, or an execution backend that has no
              consumer.
``2``         A second signal arrived and in-flight deliveries were cancelled.
============  ==================================================================

The in-memory queue backend is rejected for the same reason as ``run``: its
records never leave the process that created them, so a separate consumer would
find nothing while looking healthy.

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
   expired            1
   total            127

Options:

* ``--queue NAME`` — count only records belonging to that queue.
* ``--json`` — emit a single JSON object with the eight keys
  (``pending``, ``scheduled``, ``running``, ``completed``, ``failed``,
  ``cancelled``, ``expired``, ``total``). Output uses
  ``sqlspec.utils.serializers.to_json`` when available for symmetry with
  the camelCase wire format; falls back to stdlib ``json``.

Exit codes: ``0`` on success, ``1`` on backend error.

``litestar queues scheduler-health``
====================================

Exits with a nonzero code when the configured canary task has no recent
completion record. A canary is a small recurring task used as a health check.

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

``litestar queues run-maintenance``
===================================

Runs one bounded maintenance pass — external-execution reconciliation, stale
recovery, terminal-task retention, and durable-event retention — then exits. It
never starts a worker or executes queued work, so it is safe on a six-hour or
daily external schedule.

.. code-block:: console

   $ litestar queues run-maintenance --json
   {"outcome":"completed","acquired":true,"duration_ms":41.2,"phases":[...]}

It takes ``--phase [external|stale|terminal|events]`` (repeatable) and
``--json``. Every threshold, limit, and retention window comes from
:attr:`QueueConfig.maintenance`, so no flag can introduce a destructive cutoff.

:doc:`maintenance` is the operator guide and owns the phase reference, the
scheduling advice, and the exit codes.

``discover_tasks``
==================

Applications with an ``app.domain.<x>.jobs/`` layout can use
:func:`~litestar_queues.discover_tasks` to import every module under each
``jobs`` package at startup. This replaces a manual list in
:attr:`QueueConfig.task_modules`:

.. code-block:: python

   from litestar import Litestar

   from litestar_queues import QueueConfig, QueuePlugin, discover_tasks


   def create_app() -> Litestar:
       discover_tasks("app.domain")
       return Litestar(plugins=[QueuePlugin(QueueConfig(queue_backend="memory"))])

Signature:

.. autofunction:: litestar_queues.discover_tasks
   :noindex:

The default ``subpackage="jobs"`` matches modules with a ``jobs`` segment in
their dotted path, such as ``app.domain.billing.jobs.send_invoice``. The
function returns a sorted tuple of unique, fully qualified task names found in
the registry. You can use it in logs or metric labels.

Deployment example
==================

This shortened ``systemd`` unit shows a sidecar worker command. The same
command can run beside Granian web pods on Cloud Run or Kubernetes:

.. code-block:: yaml

   # worker.service (systemd unit, abridged)
   [Service]
   Environment=LITESTAR_APP=app.asgi:app
   ExecStart=/usr/local/bin/litestar queues run --drain-timeout 60
   Restart=on-failure
   TimeoutStopSec=90

Set ``TimeoutStopSec`` higher than ``--drain-timeout``. Otherwise, the service
manager may send ``SIGKILL`` while the worker is still shutting down.

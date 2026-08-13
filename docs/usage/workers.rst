===========
Run workers
===========

A worker is the process that claims queued records and runs your task
functions. The first decision is where that process lives, and the rest of
this page is what you tune once it is running.

Choose where the worker runs
============================

``WorkerConfig.placement`` names the process that owns the worker. There are
three choices and no fallback between them.

.. list-table::
   :header-rows: 1

   * - Placement
     - Who starts the worker
     - How many it starts
     - Storage
   * - ``server``
     - the Litestar CLI server lifespan
     - one per ``litestar run``
     - ephemeral or persistent
   * - ``asgi``
     - the application lifespan
     - one per ASGI process
     - memory or persistent
   * - ``external``
     - nobody; you do
     - none
     - persistent for ``litestar queues run``

The count is what the application starts for itself, not a ceiling. Managed
placements (``server`` and ``asgi``) reject ``execution_backend="immediate"``,
because inline execution leaves a worker with nothing to claim.

Server placement (the default)
------------------------------

.. code-block:: python

   from litestar_queues import QueueConfig

   queue_config = QueueConfig()

``QueueConfig()`` uses ``placement="server"``, so one ``litestar run``
invocation starts exactly one worker in its own fresh process, alongside a
private temporary SQLite database. Nothing to install, nothing to configure,
and the worker never competes with your web workers for the event loop:

.. code-block:: bash

   litestar --app app:app run

Server placement needs an explicit application path, either ``--app`` or
``LITESTAR_APP``, because the worker process loads the application itself.
It also needs Litestar's CLI: starting a raw ASGI server or a ``TestClient``
fails immediately rather than serving traffic against a queue nothing drains.

The default database is deliberately ephemeral. It is created when the server
starts and removed when it stops, so queued work does not survive a restart.
It has no listener or configurable path, cannot be attached by standalone CLI
commands, and is unsupported on network storage. Choose a backend from
:doc:`backends` as soon as you need durability.

One worker per ASGI process
---------------------------

.. code-block:: python

   from litestar_queues import QueueConfig, WorkerConfig

   queue_config = QueueConfig(
       queue_backend="memory", worker=WorkerConfig(placement="asgi")
   )

Every ASGI process runs its own worker inside the application lifespan. This
multiplies with your web-worker count, so four Uvicorn workers means four
queue workers. It is the right choice for a single-process development server
using ``queue_backend="memory"``, and a deliberate one everywhere else.

Standalone workers
------------------

Choose a shared, persistent queue backend, declare that nothing starts
automatically, and run the same Litestar application as a worker service:

.. code-block:: python

   from litestar_queues import QueueConfig, WorkerConfig

   queue_config = QueueConfig(
       queue_backend="redis", worker=WorkerConfig(placement="external")
   )

.. code-block:: bash

   LITESTAR_APP=app:app litestar queues run --drain-timeout 30

Process only selected queues or override concurrency:

.. code-block:: bash

   LITESTAR_APP=app:app litestar queues run \
     --queue reports --queue email --max-concurrency 4

This command refuses process-local storage. Memory lives inside the process
that created it, and the ephemeral database belongs to the ``litestar run``
invocation that created it, so neither is visible to a separate worker.

Add more workers
----------------

``placement`` names the worker your application starts for itself, not a limit
on how many workers may exist. Once you are on a persistent backend you can add
standalone workers to any placement:

.. code-block:: bash

   # one worker from the server invocation, plus three more elsewhere
   litestar --app app:app run
   LITESTAR_APP=app:app litestar queues run   # x3, on other hosts or containers

They all claim from the same backend, so the total worker count is the built-in
one plus however many you start.

Run more tasks at once
======================

``max_concurrency`` is the worker-wide ceiling on tasks executing at the same
time. Use ``queue_concurrency`` to cap individual queues below it:

.. code-block:: python

   from litestar_queues import QueueConfig, WorkerConfig

   queue_config = QueueConfig(
       worker=WorkerConfig(
           max_concurrency=8,
           queue_concurrency={"email": 1, "reports": 2},
       )
   )

Both are local limits on one worker, not distributed fleet semaphores. To bound
a shared resource across a fleet, run fewer workers or give the constrained work
its own queue and its own worker.

``batch_size`` (default ``10``) is the largest number of records the worker asks
for per claim. Raise it when tasks are short and you want fewer round trips to
the backend; a backend may return fewer records than requested, and doing so is
normal rather than an error.

.. _worker-wakeups:

Pick up new work faster
=======================

When no work is available, the worker waits instead of spinning. It wakes on a
backend notification, on a timeout, or on shutdown. ``poll_interval`` is the
starting timeout:

.. code-block:: python

   from litestar_queues import QueueConfig, WorkerConfig

   queue_config = QueueConfig(worker=WorkerConfig(poll_interval=0.25))

After an empty cycle the worker multiplies its wait by
``poll_backoff_multiplier`` up to ``poll_backoff_max``, so an idle queue stops
hammering the backend. Claimed work, a backend notification, or worker startup
resets it immediately. The defaults are ``poll_interval=0.1``,
``poll_backoff_max=30.0``, ``poll_backoff_multiplier=2.0``, and
``poll_jitter=0.15``.

Reach for these when work sits queued longer than you want:

- **Your backend sends notifications.** Redis, Valkey, and SQLSpec on
  PostgreSQL push a hint the moment work is enqueued, so pickup is prompt no
  matter what the polling numbers say. Check which of your choices support that
  in the wakeup matrix in :doc:`backends`, and prefer switching backends over
  tuning intervals.
- **Your backend polls.** Then ``poll_backoff_max`` is the worst-case delay
  before a worker notices work. Set it no higher than the latency your service
  can tolerate, or ``None`` for fixed-interval polling at ``poll_interval``.
- **You need a lower floor.** Lower ``poll_interval``. Every reduction costs
  backend round trips on an idle queue, so change it only when a measured
  latency target requires it.

Notifications are hints, not state. They can arrive late, be coalesced, or not
arrive at all; every cycle checks durable queue state before it waits, so a
later polling pass still claims the task. They are also unrelated to
:doc:`events` delivery — a Redis queue backend does not configure Redis
Channels for your SSE or WebSocket consumers.

Keep running work alive
=======================

A worker heartbeats each record it is running so other workers can tell a live
task from an abandoned one:

.. code-block:: python

   from litestar_queues import QueueConfig, WorkerConfig

   queue_config = QueueConfig(
       worker=WorkerConfig(
           heartbeat_interval=15,
           stale_after=120,
           stale_check_interval=30,
       ),
   )

All three are seconds. ``heartbeat_jitter_fraction`` (default ``0.1``) adds up
to that fraction of positive random delay to each interval so a fleet does not
write in lockstep; set it to ``0.0`` for an exact fixed interval.

Heartbeat timestamps are automatic for every running task. ``beat(detail)``
only replaces the latest short diagnostic string and does not change the
cadence.

Workers default to the identity ``worker-{pid}``. Set ``WorkerConfig.id`` when
process IDs may repeat across hosts or preforked processes; the ID appears in
logs, metrics, and task events.

If heartbeat writes compete with queue traffic, SQLSpec can route heartbeat-only
writes through ``heartbeat_pool_config`` and Advanced Alchemy through an
app-owned ``heartbeat_session_maker``. Both must point at the same database as
normal queue operations.

.. _worker-recovery:

Recover work whose worker died
==============================

Stale recovery is **off by default**. Set ``stale_after`` to turn it on: a
running record whose heartbeat is older than that is returned to the queue if it
has retries left, and otherwise ends with a stale failure. A shared lock lets
only one worker at a time run the check. A task may turn off stale requeueing or
register ``on_stale_failure`` for cleanup.

``QueueConfig.stale_requeue_priority`` decides the priority recovered work
re-enters with:

.. code-block:: python

   from litestar_queues import QueueConfig

   QueueConfig(stale_requeue_priority="preserve")        # keep the original priority (the default)
   QueueConfig(stale_requeue_priority=4)                 # ceiling clamp
   QueueConfig(stale_requeue_priority=lambda p: p - 1)   # map old priority to new

A ceiling clamp protects a queue from a record that crashes its worker
repeatedly, but it also inverts priority: work enqueued at priority ``9``
re-enters at the ceiling and can then be starved indefinitely by ordinary
priority-``5`` inflow. Reach for a clamp only when that trade is one you want,
and pair it with a real ``retries`` budget on the task. A callable that returns anything
other than an integer fails the sweep loudly rather than silently clamping.

A recovered record is taken over by another worker, which leaves the original
attempt still running. ``WorkerConfig.cancel_on_claim_loss`` (default ``True``)
cancels that attempt as soon as its heartbeat is rejected, so its side effects
stop instead of racing the replacement. Set it to ``False`` only when a task
must always finish what it started; its terminal write is rejected either way,
because the record now belongs to another worker.

If work is stuck ``running`` after a crash, check heartbeat timestamps, stale
thresholds, backend connectivity, and whether at least one worker has
``stale_after`` set at all. ``litestar queues status`` prints the counts, and
task errors are readable after :meth:`~litestar_queues.TaskResult.refresh`.

Worker recovery runs continuously while a worker runs. For an infrequent,
finite recovery pass that also applies retention, use :doc:`maintenance`
instead.

Shut a worker down
==================

By default, unfinished work remains ``running`` for stale recovery. Set
``WorkerConfig.requeue_on_shutdown=True`` to return an attempt to ``pending``
after its coroutine accepts cancellation and unwinds. Tasks can override this
with ``@task(requeue_on_shutdown=True)`` or ``False`` and must be idempotent.

Each requeue is counted in the record's ``interruptions`` metadata and does not
spend a retry attempt. ``WorkerConfig.max_interruptions`` (default ``3``) bounds
that: once an attempt has been interrupted that many times, the next
interruption goes through the ordinary retry policy instead, so a task that is
restarted forever eventually fails rather than cycling.

The escalation ladder:

#. **First signal** — stop claiming and drain for ``graceful_shutdown_timeout``.
#. **Second signal** — cancel running tasks with a ``final_cancel_timeout``
   budget and arm the hard-exit watchdog.
#. **Deadline or third signal** — the process exits with ``128 + signum``
   (``143`` for SIGTERM, ``130`` for SIGINT). ``WorkerConfig.hard_exit_timeout``
   (default ``10.0`` seconds, ``None`` to disable) is the wall-clock budget from
   forced shutdown to that exit.

Tasks still alive after ``final_cancel_timeout`` are logged with their ids and
their heartbeats are cleared, so a stale sweep can reclaim them immediately
instead of waiting out a full heartbeat age. That handoff is only useful when
``stale_after`` is set.

Server placement must become ready within ``startup_timeout``, and shutdown
finally escalates to bounded process-tree termination if the child cannot exit.
Windows uses console Ctrl+C or Ctrl+Break semantics. :doc:`cli` lists the exit
codes ``litestar queues run`` returns.

Telemetry for waits, wakeups, claims, and heartbeats is listed in
:doc:`../reference/observability`.

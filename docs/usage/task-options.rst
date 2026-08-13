============
Task options
============

Set behavior that applies to every enqueue on the decorator:

.. code-block:: python

   from litestar_queues import task


   @task(
       "reports.render",
       queue="reports",
       retries=2,
       timeout=120,
       priority=5,
       run_after=10,
       key="daily-report",
   )
   async def render_report(report_id: str) -> str:
       return report_id

``retries`` is the number of retries after the first attempt. ``timeout`` is
the maximum run time in seconds. Workers claim lower numeric priority values
first. ``run_after`` delays when the task can run. ``key`` names the logical
job and prevents more than one active record for that key.

Queued deadlines, runtime timeouts, and retention
=================================================

Use ``expires_in`` when a task is no longer useful if it waits too long before
starting:

.. code-block:: python

   @task("reports.refresh", expires_in=300)
   async def refresh_report(report_id: str) -> None:
       print(f"refreshing {report_id}")


   result = await queue_service.enqueue(refresh_report, "report-123")

The deadline starts when the record becomes runnable. For an immediately due
record, that is its enqueue time. For a delayed record, it is
``scheduled_at``:

.. code-block:: python

   result = await queue_service.enqueue(
       refresh_report,
       "report-123",
       run_after=600,
       expires_in=120,
   )

This record may wait ten minutes before becoming runnable, then has two minutes
to start. The stored ``expires_at`` is an absolute UTC timestamp. Pass an
absolute deadline when the caller already owns one:

.. code-block:: python

   from datetime import datetime, timezone

   result = await queue_service.enqueue(
       refresh_report,
       "report-123",
       expires_at=datetime(2026, 7, 26, 12, 0, tzinfo=timezone.utc),
   )

``expires_in`` and ``expires_at`` are mutually exclusive. A task-level
``expires_in`` is the default only when the enqueue call supplies neither
option. The same stored deadline fences every retry claim; it does not restart
for each attempt. If the deadline passes before a claim, the backend records
terminal status ``expired`` and the worker publishes ``task.expired``. A task
that has already started continues running.

Keep these three clocks separate:

.. list-table::
   :header-rows: 1
   :widths: 24 28 48

   * - Concern
     - Configure with
     - Effect
   * - Queued deadline
     - ``expires_in`` or ``expires_at``
     - Prevents a late claim; the record becomes ``expired``.
   * - Runtime timeout
     - ``timeout``
     - Bounds the running task body. It does not age a queued record.
   * - Terminal retention
     - ``QueueMaintenanceConfig.terminal_retention``
     - Deletes old terminal records, including ``expired`` records. It does not
       stop queued or running work.

Workers sweep overdue records every
``WorkerConfig.expiry_check_interval`` seconds (default ``60``) so an unclaimed
record becomes observable as ``expired``. Set the interval to ``None`` to
disable the sweep. The backend claim fence remains authoritative and still
prevents overdue work from running.

Synchronous tasks
=================

A task function may be ``async def`` or a plain ``def``. Async tasks run
directly on the worker's event loop. Synchronous tasks are offloaded to a
worker thread by default, so a blocking call cannot stall the loop that also
runs heartbeats, the claim loop, and event publishing:

.. code-block:: python

   import time

   from litestar_queues import task


   @task("reports.render")
   def render_report(report_id: str) -> str:
       time.sleep(2)  # stand-in for a blocking PDF library call
       return report_id

``sync_to_thread`` makes that choice explicit, using the same vocabulary as
Litestar's route handlers:

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Value
     - Behavior
   * - unset (default)
     - Offload the synchronous function to a thread.
   * - ``sync_to_thread=True``
     - Offload the synchronous function to a thread.
   * - ``sync_to_thread=False``
     - Run the synchronous function inline on the event loop.

Set ``sync_to_thread=False`` only when the function is guaranteed not to
block -- it returns a cached value, formats a string, or does pure arithmetic.
Offloading has a small cost, and avoiding it for genuinely non-blocking work is
the only reason to opt out:

.. code-block:: python

   @task("reports.label", sync_to_thread=False)
   def label(report_id: str) -> str:
       return f"report:{report_id}"

The default differs from Litestar, which runs unmarked synchronous handlers
inline and warns. A queue worker shares one loop with its own liveness
machinery, so the safe path is the default here and no warning is emitted.

``sync_to_thread`` has no effect on an ``async def`` task, which always runs on
the loop. Passing it to one emits
:class:`~litestar_queues.exceptions.QueueWarning` at decoration; silence it with
``LITESTAR_WARN_SYNC_TO_THREAD_WITH_ASYNC=0``, the same variable Litestar uses.

Async detection follows Litestar's rules, so a :func:`functools.partial` around
a coroutine function and a class instance defining ``async def __call__`` both
count as async.

Sizing the thread pool
----------------------

The queue service owns its own thread pool, so synchronous tasks never contend
with other :func:`asyncio.to_thread` callers in the same process.
``sync_thread_pool_size`` caps how many run at once. Its default is the
process-usable CPU count plus four, capped at 32; Linux cgroup quotas and CPU
affinity constrain that count:

.. code-block:: python

   QueueConfig(sync_thread_pool_size=64)

Threads are created on demand, so the default is a ceiling rather than a startup
cost. Raise it when tasks are I/O-bound and spend most of their time waiting;
lower it to protect a resource that cannot take many concurrent callers, such as
a small database connection pool.

``sync_thread_name_prefix`` names those threads for profiling. The pool opens
with the service and shuts down when it closes. Calling a task directly outside
any service -- ``await render_report("id")`` -- has no pool to draw on and uses
the loop's default executor instead.

Override one enqueue
====================

.. code-block:: python

   result = await queue_service.enqueue(
       render_report,
       "report-123",
       retries=4,
       timeout=600,
       priority=1,
       run_after=30,
       key="report:report-123",
       metadata={"requested_by": "user-42"},
       description="Render the monthly report",
   )

Queue records store JSON-like arguments and metadata. A persistent backend can
store only values supported by its serializer.

Execution overrides
===================

``execution_backend`` selects inline, local-worker, or external execution for
one task. ``execution_profile`` lets an external backend select a configured
profile. These options change placement, not queue persistence.

Use a key when duplicate requests should share one active record. If an active
record already has that key, enqueueing returns that record. A task in a final
state does not reserve the key forever, so the next enqueue can create a new
record. Omit the key, or generate a different one, when concurrent
invocations are wanted.

Task uniqueness
===============

Opt into identity without hand-building keys with ``unique_by`` and, only when
you need it, ``unique_until``.

- ``unique_by="task"`` derives the identity from the registered task name.
  Calls share one active record unless an explicit or configured key wins.
- ``unique_by="arguments"`` derives the identity from the registered task name
  plus the normalized call. Positional and keyword forms of the same call, and
  applied defaults, produce the same identity. The injected ``_task_context``
  is excluded, and a versioned canonical JSON payload is hashed with SHA-256.
  It never uses ``pickle`` or ``repr()``.

.. code-block:: python

   from litestar_queues import task


   @task("reports.refresh", unique_by="task")
   async def refresh_report(report_id: str) -> None:
       print(f"refreshing {report_id}")


   @task("reports.render", unique_by="arguments")
   async def render_report(report_id: str, *, fmt: str = "pdf") -> None:
       print(f"rendering {report_id} as {fmt}")

Identity precedence and lifetime
--------------------------------

Identity is selected by strict precedence. The first applicable row wins. The
explicit, configured, and task-only paths never bind, serialize, or hash
arguments.

.. list-table::
   :header-rows: 1
   :widths: 30 35 35

   * - Configuration
     - Identity
     - Argument handling
   * - explicit enqueue ``key``
     - the exact enqueue-time key
     - not inspected
   * - configured task ``key``
     - the exact decorator key
     - not inspected
   * - ``unique_by="task"``
     - a versioned hash of the registered task name
     - not inspected
   * - ``unique_by="arguments"``
     - a versioned hash of the task name plus normalized call
     - bound, normalized, JSON-encoded, and hashed
   * - no ``key``, no ``unique_by``
     - none; every enqueue is a new record
     - not inspected

``unique_until`` defaults to ``"terminal"``. It sets the lifetime of whichever
identity wins the precedence above, including an explicit enqueue key:

- ``"terminal"`` allows a new record after the current one reaches a final
  state.
- ``"forever"`` retains a reservation until an administrator resets it.

Spell out ``unique_until="forever"`` only when that retained identity is
required:

.. code-block:: python

   @task("imports.once", unique_by="arguments", unique_until="forever")
   async def import_once(object_key: str) -> None:
       print(f"importing {object_key}")

A ``forever`` identity blocks every later enqueue of that identity -- even after
the original record completes and is cleaned up. The only way to allow a new
enqueue is an explicit administrative reset with the exact effective key:

.. code-block:: python

   await queue_service.reset_task_identity(effective_key)

Rules and validation
--------------------

- A configured ``key`` is already an identity, so combining a configured ``key``
  with ``unique_by`` is rejected at decoration.
- ``unique_until="forever"`` requires an identity; setting it with neither a
  configured ``key`` nor ``unique_by`` is rejected.
- With no ``key`` and no ``unique_by``, uniqueness is disabled: every call is a
  separate, independently claimable record.
- Schedules keep their internal ``scheduled:{task_name}`` identity and are never
  rehashed or given uniqueness reservations.
- Hashing keeps only the identity key small. Large task payloads still belong in
  object or database storage; pass a stable id. Set
  ``QueueConfig.max_argument_identity_bytes`` to reject oversized
  ``unique_by="arguments"`` payloads with
  :class:`~litestar_queues.exceptions.TaskIdentityTooLargeError`.

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
   async def refresh_report(report_id: str) -> None: ...


   @task("reports.render", unique_by="arguments")
   async def render_report(report_id: str, *, fmt: str = "pdf") -> None: ...

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
- ``"forever"`` retains a tombstone until an administrator resets it.

Spell out ``unique_until="forever"`` only when that retained identity is
required:

.. code-block:: python

   @task("imports.once", unique_by="arguments", unique_until="forever")
   async def import_once(object_key: str) -> None: ...

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
  rehashed or given uniqueness tombstones.
- Hashing keeps only the identity key small. Large task payloads still belong in
  object or database storage; pass a stable id. Set
  ``QueueConfig.max_task_payload_bytes`` to reject oversized
  ``unique_by="arguments"`` payloads with
  :class:`~litestar_queues.exceptions.TaskPayloadTooLargeError`.

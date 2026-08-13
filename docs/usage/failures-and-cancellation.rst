=========================
Failures and cancellation
=========================

Retry ordinary failures by setting ``retries``:

.. code-block:: python

   from litestar_queues import non_retryable, task


   @task("billing.charge", retries=3, timeout=60)
   async def charge(invoice_id: str) -> None:
       if invoice_id.startswith("invalid-"):
           non_retryable("Invoice cannot be charged")

An ordinary exception is retried while attempts remain. ``non_retryable()``
raises :class:`~litestar_queues.NonRetryableError` and moves directly to a
terminal failure. Inspect ``TaskResult.error`` after refreshing the result.

Use ``retry_backoff=5`` for a fixed delay, or
``RetryBackoff(initial_delay=1, multiplier=2, max_delay=30)`` for capped
exponential backoff. A retry receives a fresh queue timestamp.

Cancel pending work
===================

.. code-block:: python

   cancelled = await queue_service.cancel_task(result.id)

You can cancel pending and scheduled records before a worker claims them. Bulk
cancellation can filter by task name, queue, keyword arguments, or metadata.
Repeated calls return ``False`` after the first successful transition and do
not publish another ``task.cancelled`` lifecycle event.

Cooperative running cancellation
================================

A running task can stop itself with ``job_cancelled("reason")`` or raise
:class:`~litestar_queues.JobCancelledError`. This records ``cancelled`` and
does not retry. ``await queue_service.cancel_task(task_id,
include_running=True)`` also permits the durable state transition for a running
record. The default remains ``False`` so an ordinary cancellation call cannot
silently overwrite active work. Running cancellation is cooperative: the task
must check for cancellation and release its resources safely.

How a running cancellation reaches the worker
=============================================

The durable status write is authoritative. Every worker reconciles its running
tasks against stored status on a fixed cadence
(``WorkerConfig.cancellation_poll_interval``, one second by default), so a
cancellation always lands even if nothing else works.

On top of that, cancelling a running record publishes a worker-control hint so
the owning worker stops waiting and reconciles immediately. This matters most
when a worker is at ``max_concurrency``: it has nothing to claim, so it sits in
an adaptive polling wait that can grow to ``poll_backoff_max`` (thirty seconds
by default). The hint interrupts that wait.

.. list-table::
   :header-rows: 1

   * - Backend
     - Hint transport
   * - Redis, Valkey
     - a dedicated pub/sub control channel
   * - SQLSpec on PostgreSQL
     - the events channel, ``LISTEN``/``NOTIFY``-backed
   * - memory
     - an in-process event
   * - everything else
     - none; the durable poll is the only path

Hints are lossy by contract. A dropped hint costs latency, never correctness,
so a backend that cannot deliver one simply keeps polling. Backends adding a
transport implement ``notify_worker_control`` and ``wait_for_worker_control``
and must preserve that contract: no cancellation outcome may depend on a hint
arriving.

Inside a task, ``current_task_context().is_cancelled`` exposes the cooperative
token; ``wait_cancelled()`` waits for it and ``raise_if_cancelled()`` raises
``JobCancelledError``. Threaded synchronous work must use these checkpoints.

Timeouts use normal failure handling. Make external calls cancellable and safe
to repeat so a retry does not corrupt partially completed work.

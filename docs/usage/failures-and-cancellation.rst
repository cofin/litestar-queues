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

Cancel pending work
===================

.. code-block:: python

   cancelled = await queue_service.cancel_task(result.id)

Pending and scheduled records can be cancelled before claim. Bulk cancellation
can filter by task name, queue, keyword arguments, or metadata.

Cooperative running cancellation
================================

A running task can terminate itself with ``job_cancelled("reason")`` or raise
:class:`~litestar_queues.JobCancelledError`. This records ``cancelled`` and
does not retry. Backend cancellation with ``include_running=True`` changes the
record state, but user code must still cooperate with cancellation and release
its resources safely.

Timeouts use normal failure handling. Make external calls cancellable and
idempotent so a retry can safely resume after partial work.

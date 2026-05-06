=========
Schedules
=========

Tasks can declare a recurring interval or cron schedule. Scheduled tasks are
registered in process and synchronized to the queue backend during startup when
``QueueConfig.initialize_schedules`` is enabled.

Interval Schedules
==================

Use ``interval`` for fixed-delay recurring work:

.. code-block:: python

   from datetime import timedelta
   from litestar_queues import task


   @task("reports.refresh", interval=timedelta(minutes=15), jitter=30)
   async def refresh_reports() -> None:
       ...

Cron Schedules
==============

Use ``cron`` for calendar-based schedules:

.. code-block:: python

   @task("billing.close-day", cron="0 0 * * *", timezone="UTC")
   async def close_billing_day() -> None:
       ...

Cron aliases such as ``@hourly``, ``@daily``, ``@weekly``, ``@monthly``, and
``@yearly`` are supported.

Startup Synchronization
=======================

When startup synchronization runs, Litestar Queues creates a pending queue
record keyed as ``scheduled:<task-name>``. If an active record already exists
with matching schedule metadata, it is reused. If schedule metadata changed,
the old active record is cancelled and a replacement is created.

Completed and failed scheduled records are rescheduled after execution using the
next run time from the persisted schedule metadata. This keeps scheduling data
close to the queue record and allows durable backends to recover after process
restarts.

Configuration
=============

.. code-block:: python

   config = QueueConfig(
       task_modules=("app.tasks",),
       initialize_schedules=True,
       execution_backend="local",
       start_worker=True,
   )

Set ``initialize_schedules=False`` when schedules are initialized by a separate
worker or management command.

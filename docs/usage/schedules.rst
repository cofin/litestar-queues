=========
Schedules
=========

Tasks can declare a recurring interval or cron schedule. The process registers
these tasks. At startup, it writes their schedules to the queue backend when
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

Supported Cron Syntax
---------------------

Litestar Queues accepts standard five-field cron expressions:

.. code-block:: text

   minute hour day-of-month month day-of-week

The supported grammar includes:

- ``*`` wildcards;
- comma lists, such as ``JAN,MAR``;
- numeric and named ranges, such as ``9-17`` and ``MON-FRI``;
- positive steps, such as ``*/15``;
- named months and weekdays;
- Sunday as either ``0`` or ``7``; and
- ``?`` in ``day-of-month`` or ``day-of-week`` to mean no specific value.

When both ``day-of-month`` and ``day-of-week`` are restricted, either field may
match. For example, ``0 0 1 * MON`` runs at midnight on the first day of the
month and on Mondays.

Unsupported Cron Syntax
-----------------------

Litestar Queues rejects cron extensions that are not part of the v1 grammar,
including:

- seconds fields;
- year fields;
- ``@reboot``;
- ``L``, ``W``, and ``#`` day modifiers;
- empty list items;
- reversed ranges;
- invalid names or out-of-range values; and
- zero or negative step values.

Use multiple schedules or application code for calendar rules that require
unsupported cron extensions.

Startup Synchronization
=======================

During startup synchronization, Litestar Queues creates a pending record with
the key ``scheduled:<task-name>``. It reuses an active record when its schedule
metadata still matches. If the schedule changed, it cancels the old record and
creates a new one.

After a scheduled task completes or fails, Litestar Queues reads the saved
schedule metadata and creates its next run. Keeping the schedule with the
queue record lets persistent backends recover after a process restart.

Scheduled records include the same task metadata used by normal enqueue calls.
When a task does not set ``quiet_success``, schedule startup stores
``QueueConfig.quiet_success`` on the queued record. This affects only the
successful completion log line; scheduled tasks still publish lifecycle events
and event history.

Configuration
=============

.. code-block:: python

   config = QueueConfig(
       task_modules=("app.tasks",),
       initialize_schedules=True,
       execution_backend="local",
       in_app_worker=True,
   )

Set ``initialize_schedules=False`` when schedules are initialized by a separate
worker or management command.

See also
========

Use :doc:`task-options` for retry, timeout, and execution defaults on scheduled
tasks. Use :doc:`workers` to ensure exactly one intended startup path
synchronizes schedules and enough workers are running to execute due records.

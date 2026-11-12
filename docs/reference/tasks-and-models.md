# Tasks, Models & Exceptions

API reference for task registration, recurring schedule configuration, dependency hooks, persisted task models, and the exception hierarchy.

## Tasks & Schedules

::: litestar_queues.task.task

::: litestar_queues.task.Task
    options:
      inherited_members: false

::: litestar_queues.task.ScheduleConfig
    options:
      inherited_members: false

::: litestar_queues.task.RetryBackoff

::: litestar_queues.task.TaskResult
    options:
      inherited_members: false

::: litestar_queues.task.TaskUniqueBy

::: litestar_queues.task.TaskUniqueUntil

::: litestar_queues.config.TaskDependencyProvider
    options:
      inherited_members: false

::: litestar_queues.config.TaskDependencyResolver
    options:
      inherited_members: false

::: litestar_queues.task.get_task_registry

::: litestar_queues.task.get_scheduled_tasks

::: litestar_queues.task.discover_tasks

::: litestar_queues.task.load_task_modules

## Models & Statuses

::: litestar_queues.models.QueuedTaskRecord
    options:
      inherited_members: false

::: litestar_queues.models.TaskRequest
    options:
      inherited_members: false

::: litestar_queues.models.TaskReservation
    options:
      inherited_members: false

::: litestar_queues.models.TaskStatus

::: litestar_queues.models.QueueStatistics
    options:
      inherited_members: false

::: litestar_queues.models.TaskListPage
    options:
      inherited_members: false

::: litestar_queues.models.QueueBackendCapabilities
    options:
      inherited_members: false

::: litestar_queues.models.StaleTaskRecoveryResult
    options:
      inherited_members: false

::: litestar_queues.models.ScheduleAssertionResult
    options:
      inherited_members: false

::: litestar_queues.models.HeartbeatTouch
    options:
      inherited_members: false

::: litestar_queues.models.HeartbeatTouchResult
    options:
      inherited_members: false

## Exceptions & Terminal Helpers

::: litestar_queues.exceptions.non_retryable

::: litestar_queues.exceptions.job_cancelled

::: litestar_queues.exceptions.QueueError
    options:
      inherited_members: false

::: litestar_queues.exceptions.QueueWarning
    options:
      inherited_members: false

::: litestar_queues.exceptions.QueueConfigurationError
    options:
      inherited_members: false

::: litestar_queues.exceptions.QueueDispatchError
    options:
      inherited_members: false

::: litestar_queues.exceptions.QueueDispatchRepairError
    options:
      inherited_members: false

::: litestar_queues.exceptions.TaskIdentityError
    options:
      inherited_members: false

::: litestar_queues.exceptions.TaskIdentityTooLargeError
    options:
      inherited_members: false

::: litestar_queues.exceptions.QueueEventBufferFull
    options:
      inherited_members: false

::: litestar_queues.exceptions.JobCancelledError
    options:
      inherited_members: false

::: litestar_queues.exceptions.NonRetryableError
    options:
      inherited_members: false

::: litestar_queues.exceptions.MissingDependencyError
    options:
      inherited_members: false

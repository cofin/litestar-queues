# Plugin, Config & Service

API reference for the Litestar plugin, root configuration dataclasses, queue service, runtime namespace, and deferred post-response task adapter.

## Plugin

::: litestar_queues.plugin.QueuePlugin
    options:
      inherited_members: false

## Configuration

::: litestar_queues.config.QueueConfig
    options:
      inherited_members: false

::: litestar_queues.config.WorkerConfig
    options:
      inherited_members: false

::: litestar_queues.maintenance.QueueMaintenanceConfig
    options:
      inherited_members: false

## Service

::: litestar_queues.service.QueueService
    options:
      inherited_members: false

::: litestar_queues.task.get_default_service

::: litestar_queues.task.set_default_service

## Runtime Namespace & Background Tasks

::: litestar_queues.namespace.QueueNamespace
    options:
      inherited_members: false

::: litestar_queues.background.QueuedBackgroundTask
    options:
      inherited_members: false

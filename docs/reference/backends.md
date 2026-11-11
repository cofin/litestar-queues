# Storage Backends

API reference for the authoritative queue storage contract (`BaseQueueBackend`), backend registry helpers, and all built-in storage backend configurations and implementations.

## Backend Registry & Base Contract

::: litestar_queues.backends.get_queue_backend

::: litestar_queues.backends.get_queue_backend_class

::: litestar_queues.backends.list_queue_backends

::: litestar_queues.backends.queue_backend

::: litestar_queues.backends.base.BaseQueueBackend
    options:
      inherited_members: false
      show_source: false
      members:
        - open
        - close
        - enqueue
        - enqueue_many
        - claim_next
        - claim_many
        - claim_task
        - complete_task
        - fail_task
        - cancel_task
        - cancel_tasks
        - expire_overdue
        - requeue_stale_running
        - touch_heartbeats
        - cleanup_terminal
        - reset_identity
        - get_task
        - get_tasks
        - list_tasks
        - get_statistics

## SQLSpec Backend

::: litestar_queues.backends.sqlspec.SQLSpecBackendConfig
    options:
      inherited_members: false

::: litestar_queues.backends.sqlspec.SQLSpecWorkerWakeupConfig
    options:
      inherited_members: false

::: litestar_queues.backends.sqlspec.SQLSpecQueueBackend
    options:
      inherited_members: false
      show_source: false
      members:
        - __init__
        - open
        - close
        - create_schema

::: litestar_queues.backends.sqlspec.stores.base.SQLSpecQueueStore
    options:
      inherited_members: false
      show_source: false
      members:
        - __init__
        - adapter_name

::: litestar_queues.backends.sqlspec.stores.factory.create_queue_store

::: litestar_queues.backends.sqlspec.configure_queue_migration_extension

## Advanced Alchemy Backend

::: litestar_queues.backends.advanced_alchemy.SQLAlchemyBackendConfig
    options:
      inherited_members: false

::: litestar_queues.backends.advanced_alchemy.SQLAlchemyBackend
    options:
      inherited_members: false
      show_source: false
      members:
        - __init__
        - open
        - close

::: litestar_queues.backends.advanced_alchemy.mixins.QueueTaskModelMixin
    options:
      inherited_members: false
      show_source: false

::: litestar_queues.backends.advanced_alchemy.mixins.QueueEventHistoryModelMixin
    options:
      inherited_members: false
      show_source: false

::: litestar_queues.backends.advanced_alchemy.mixins.QueueMaintenanceModelMixin
    options:
      inherited_members: false
      show_source: false

::: litestar_queues.backends.advanced_alchemy.mixins.QueueTaskReservationModelMixin
    options:
      inherited_members: false
      show_source: false

## Redis & Valkey Backends

::: litestar_queues.backends.redis.RedisBackendConfig
    options:
      inherited_members: false

::: litestar_queues.backends.redis.RedisQueueBackend
    options:
      inherited_members: false
      show_source: false
      members:
        - __init__
        - open
        - close

::: litestar_queues.backends.valkey.ValkeyBackendConfig
    options:
      inherited_members: false

::: litestar_queues.backends.valkey.ValkeyQueueBackend
    options:
      inherited_members: false
      show_source: false
      members:
        - __init__
        - open
        - close

## Ephemeral & In-Memory Backends

::: litestar_queues.backends.ephemeral.backend.EphemeralQueueBackend
    options:
      inherited_members: false
      show_source: false
      members:
        - __init__
        - open
        - close

::: litestar_queues.backends.memory.backend.InMemoryQueueBackend
    options:
      inherited_members: false
      show_source: false
      members:
        - __init__
        - open
        - close

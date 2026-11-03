# Durable Event History

Configuring [`EventHistoryConfig`](../reference/events.md#litestar_queues.events.EventHistoryConfig) on `QueueEventsConfig.history` enables backend-owned durable event persistence ([`QueueEventLog`](../reference/events.md#litestar_queues.events.QueueEventLog)), paginated history queries, per-stage duration summaries, and commit-before-live delivery coordination.

## Runnable Example

```python title="docs/examples/events/event_history_query.py"
--8<-- "docs/examples/events/event_history_query.py"
```

## Commit-Before-Live Guarantee

When both `QueueEventsConfig.history` and `QueueEventsConfig.delivery` (or `stream`) are enabled, `QueueEventPublisher` routes events through `event_log.publish_event_after_commit(...)`:

1. **Buffered Batch Persistence**: Events enter the history buffer (`batch_size=20`, `flush_interval=1.0s`, `max_pending=2000`) and are written to the storage backend in batches.
2. **Terminal Lifecycle Barrier (`barrier=True`)**: Terminal lifecycle events (`task.completed`, `task.failed`, `task.cancelled`, `task.expired`, `task.stale_failed`) force an immediate history flush and hold live `ChannelsPlugin` release until the database commit succeeds.
3. **Zero Read-After-Notification Races**: When a browser client receives a `task.completed` frame over SSE or WebSocket and immediately fetches the task's event history or stage summary via HTTP, the complete event trail is guaranteed to be committed and queryable in storage.

## Storage Backend Support Matrix

| Storage Backend | History Storage Structure | Default Capacity / Retention |
| :--- | :--- | :--- |
| **SQLSpec (`SQLSpecBackendConfig`)** | `queue_task_event_history` table (all 11 SQL dialects) | Bounded by `QueueMaintenanceConfig.event_retention_rules` |
| **Advanced Alchemy (`SQLAlchemyBackendConfig`)** | `QueueEventHistoryModel` (`QueueEventHistoryModelMixin`) | Bounded by `QueueMaintenanceConfig.event_retention_rules` |
| **Redis & Valkey (`RedisBackendConfig`, `ValkeyBackendConfig`)** | Per-event hashes + indexed sorted sets under `{prefix}:events:*` | Bounded by `QueueMaintenanceConfig.event_retention_rules` |
| **Ephemeral SQLite (`"ephemeral"`)** | `queue_task_event_history` table in temp SQLite database | Bounded by `QueueMaintenanceConfig.event_retention_rules` |
| **In-Memory (`"memory"`)** | Ring-bounded in-memory log (`EventHistoryConfig.memory_capacity=1000`) | Automatically evicts oldest records past `memory_capacity` + supports `cleanup_events()` |

## Querying Event History (`QueueEventQuery`)

Retrieve the active `QueueEventLog` via `queue_service.get_event_log()` and call `await event_log.query_events(query, extra=...)`:

| Filter Field on `QueueEventQuery` | Type | Description |
| :--- | :--- | :--- |
| `task_id` | `str \| None` | Exact task UUID string filter |
| `task_name` | `str \| None` | Registered task name equality filter |
| `event_type` | `str \| None` | Event type equality filter (for example `"task.progress"` or `"task.failed"`) |
| `scope` | `str \| None` | Event scope filter (`"task"`, `"queue"`, `"worker"`, `"global"`, `"custom"`) |
| `scope_key` | `str \| None` | Adopter scope key equality filter (for example `"tenant:acme"`) |
| `entity` | `str \| None` | Canonical entity key (`"<type>:<id>"` or `QueueEventEntityRef`) |
| `level` | `str \| None` | Severity level filter (`"debug"`, `"info"`, `"warning"`, `"error"`, `"critical"`) |
| `order` | `Literal["asc", "desc"]` | Sort direction by stable `(occurred_at, sequence, created_at, event_id)` order key |
| `limit` | `int \| None` | Page size (`None` returns all matching rows) |
| `offset` | `int` | Zero-based row offset (`0` default) |

In addition to `QueueEventQuery` fields, pass `extra={"actor_id": "...", "actor_type": "..."}` (or any custom `EventHistoryExtraColumn` name) to `query_events()` to filter by actor attribution or adopter-defined columns. `query_events()` returns an `OffsetPagination[QueueEventLogRecord]` with `.items`, `.total`, `.limit`, and `.offset`.

## Per-Stage Aggregation (`summarize_stages`)

When tasks include `"stage"` (and optional `"duration_ms"`) keys inside `payload`, call `await event_log.summarize_stages(QueueEventQuery(task_id=...))` to aggregate matching records into [`QueueEventStageSummary`](../reference/events.md#litestar_queues.events.QueueEventStageSummary) objects:

- `stage`: Stage name string (or `None` for unstaged events)
- `event_count`: Total events recorded in that stage
- `total_duration_ms`: Sum of `duration_ms` across events in that stage
- `first_event_at` / `last_event_at`: Earliest and latest `occurred_at` UTC timestamps
- `latest_sequence` / `latest_message`: Sequence number and message from the newest event in the stage
- `worst_level`: Highest-severity level observed in the stage (`critical` > `error` > `warning` > `info` > `debug`)

!!! note "Unpaginated Query Requirement"
    `summarize_stages()` aggregates across the full matching event set and rejects queries that set `limit`, `offset`, or `order="desc"` with `QueueConfigurationError`.

## Retention Rules & Maintenance Cleanup

Configure ordered [`QueueEventRetentionRule`](../reference/events.md#litestar_queues.events.QueueEventRetentionRule) entries on `QueueMaintenanceConfig.event_retention_rules` to prune old event history during the `"events"` maintenance phase:

```python
from litestar_queues import QueueMaintenanceConfig
from litestar_queues.events import QueueEventQuery, QueueEventRetentionRule

maintenance = QueueMaintenanceConfig(
    event_retention_rules=(
        QueueEventRetentionRule(max_age=86_400.0, match=QueueEventQuery(event_type="task.progress")),
        QueueEventRetentionRule(max_age=604_800.0, match=QueueEventQuery()),
    ),
    event_limit=1000,
)
```

Rules are evaluated in tuple order; the first rule whose `match` query matches a record governs its retention window (`max_age` in seconds). Records matching no rule are retained indefinitely.

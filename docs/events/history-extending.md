# Extending Event History

You can extend durable event history in two ways:

1. **Declarative Extra Columns (`EventHistoryExtraColumn`)**: Extract domain keys (such as `tenant_id`, `organization_id`, or `workflow_id`) from `QueueEvent.payload` into first-class, indexed SQL columns on `queue_task_event_history`.
2. **Custom `QueueEventLog` Implementation**: Implement the [`QueueEventLog`](../reference/events.md#litestar_queues.events.QueueEventLog) protocol to store and query event history in an external analytics store (such as ClickHouse, BigQuery, or OpenSearch).

## Declarative Extra Columns (`EventHistoryExtraColumn`)

```python title="docs/examples/events/custom_event_log.py"
--8<-- "docs/examples/events/custom_event_log.py"
```

### Column Validation Rules

When declaring [`EventHistoryExtraColumn(name=..., source=..., indexed=...)`](../reference/events.md#litestar_queues.events.EventHistoryExtraColumn) on `EventHistoryConfig.extra_columns` and `SQLSpecBackendConfig.event_history_extra_columns`:

- **Valid Unquoted SQL Identifiers**: `name` must be a non-empty ASCII identifier starting with a letter or underscore and containing only `[A-Za-z0-9_]`.
- **No Package Column Collisions**: `name` (compared case-insensitively) cannot redeclare any built-in `queue_task_event_history` column (`event_id`, `event_type`, `task_id`, `task_name`, `queue`, `worker_id`, `execution_backend`, `execution_profile`, `actor_type`, `actor_id`, `stage`, `level`, `message`, `detail`, `progress_current`, `progress_total`, `progress_percent`, `duration_ms`, `sequence`, `occurred_at`, `created_at`, `scope`, `scope_key`, `actor`, `entity`).
- **Reserved Scoping Dimensions**: Built-in event scoping columns in `RESERVED_EVENT_HISTORY_COLUMNS` (`"actor"`, `"entity"`, `"scope"`, `"scope_key"`) are rejected upfront with a dedicated reserved-scoping error so custom projections cannot shadow event visibility filters.
- **Automatic Indexing**: Newly declared nullable `EventHistoryExtraColumn` columns are added automatically by `await backend.create_schema()` and SQLSpec `migration_mode="reconcile"` (`upgrade("head")`), and setting `indexed=True` creates or repairs a composite `(name, occurred_at)` index on `queue_task_event_history` for tenant-scoped timeline queries.

## Implementing a Custom QueueEventLog Provider

To plug in a custom event history store, implement the seven methods of the [`QueueEventLog`](../reference/events.md#litestar_queues.events.QueueEventLog) protocol:

| Method | Signature | Contract |
| :--- | :--- | :--- |
| `publish_event` | `async (self, event: QueueEvent) -> None` | Enqueue or persist a single `QueueEvent` |
| `publish_event_after_commit` | `async (self, event: QueueEvent, *, release: Callable[[], Awaitable[None]], barrier: bool = False) -> None` | Persist `event` (flushing immediately when `barrier=True`) and invoke `await release()` only after the record is durably committed |
| `flush_events` | `async (self) -> None` | Flush any buffered writes to durable storage |
| `aclose` | `async (self) -> None` | Drain pending writes and release connections during `QueueService.close()` |
| `query_events` | `async (self, query: QueueEventQuery \| None = None, *, extra: Mapping[str, str] \| None = None) -> OffsetPagination[QueueEventLogRecord]` | Filter, sort by stable order key, and paginate event records |
| `summarize_stages` | `async (self, query: QueueEventQuery \| None = None) -> list[QueueEventStageSummary]` | Call `require_unpaginated_query(query)` and return per-stage aggregations |
| `cleanup_events` | `async (self, *, before: datetime, match: QueueEventQuery \| None = None, exclude: Sequence[QueueEventQuery] = (), limit: int \| None = None) -> int` | Delete records older than `before` matching `match` and not matching any `exclude` query, up to `limit` |

!!! important "Commit-Aware Release Contract"
    Custom `QueueEventLog` implementations must implement `publish_event_after_commit` and `aclose` faithfully. Invoking `await release()` before the event is committed breaks the commit-before-live guarantee for SSE and WebSocket subscribers.

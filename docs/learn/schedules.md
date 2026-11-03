# Recurring Schedules

`litestar-queues` supports declarative interval and cron schedules directly on `@task` definitions without requiring an external scheduler daemon for local worker deployments.

## Runnable Example

```python title="docs/examples/learn/recurring_schedules.py"
--8<-- "docs/examples/learn/recurring_schedules.py"
```

## Declaring Interval & Cron Schedules

A `@task` may specify either `interval` or `cron` (specifying both raises `ValueError`):

| Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `interval` | `timedelta \| int \| float \| None` | `None` | Fixed duration (or seconds) between recurring executions |
| `cron` | `str \| None` | `None` | Standard five-field cron expression (`minute hour day-of-month month day-of-week`) |
| `timezone` | `str` | `"UTC"` | IANA timezone name (via `zoneinfo.ZoneInfo`) used to evaluate `cron` expressions |
| `initial_delay` | `timedelta \| int \| float` | `0` | Delay applied before the first `interval` occurrence when schedules are initialized |
| `jitter` | `timedelta \| int \| float` | `0` | Maximum random positive offset added to each computed next-run timestamp |

### Five-Field Cron Grammar

Cron expressions use the standard five-field syntax (`minute hour day-of-month month day-of-week`):

- **Field Ranges**: Minutes `0-59`, Hours `0-23`, Day of month `1-31`, Month `1-12`, Day of week `0-6` (or `7` for Sunday, where `0` and `7` are Sunday and `1` is Monday).
- **Operators**: Wildcards (`*`), single values (`5`), ranges (`1-5`), steps (`*/15`, `1-10/2`), and comma-separated lists (`0,15,30,45`).
- **Day Matching**: Following Vixie cron semantics, when both `day-of-month` and `day-of-week` are restricted (neither is `*`), an occurrence matches if *either* field matches the date; otherwise both must match.
- **Unsupported Extensions**: Six-field seconds, `@hourly`/`@daily` nicknames, `L`, `W`, and `#` modifiers are rejected at registration time with `ValueError`.

## Startup Synchronization & Self-Rescheduling

When `QueueConfig.initialize_schedules=True` (the default), `QueueService.initialize_schedules()` synchronizes registered [`ScheduleConfig`](../reference/tasks-and-models.md#litestar_queues.task.ScheduleConfig) entries with the storage backend during startup:

1. **Deterministic Identity Key**: Each recurring schedule uses the reserved key `scheduled:<task-name>`.
2. **Idempotent Startup Reassertion**:
   - If an active (`pending`, `scheduled`, or `running`) record with `key="scheduled:<task-name>"` already exists and its persisted `metadata["schedule"]` matches the current `ScheduleConfig`, `initialize_schedules()` reuses the existing occurrence without resetting its `scheduled_at` timestamp or retry budget.
   - If the schedule configuration changed in code (for example `cron="0 2 * * *"` changed to `cron="0 4 * * *"`), the outdated non-terminal occurrence is cancelled and replaced with a fresh `scheduled` record.
3. **Completion Rescheduling**: Whenever a scheduled occurrence reaches `completed` (or permanently fails after exhausting retries), `QueueService` automatically computes `schedule.get_next_run()` and persists the next `scheduled:<task-name>` occurrence.

## Retry Budget Preservation Across Restarts

Each scheduled occurrence snapshots `@task(..., retries=...)` into `QueuedTaskRecord.max_retries` when created. Because `initialize_schedules()` and `reassert_schedules()` preserve any active occurrence whose schedule metadata has not changed, an occurrence that is mid-retry (`retry_count > 0`) retains its consumed retry count and exponential backoff schedule across application and worker restarts.

## Scale-to-Zero & External Schedule Reassertion

In serverless deployments (`WorkerConfig(placement="external")` with Cloud Tasks or Cloud Run Jobs) where no long-lived worker process is running, trigger schedule assertion periodically through [`QueueMaintenanceService`](../execution/maintenance.md) (`litestar queues maintenance --only schedules` or the signed HTTP maintenance endpoint). `QueueService.reassert_schedules()` creates any missing `scheduled:<task-name>` occurrences and repairs external transport dispatch references.
